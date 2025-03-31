package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// 配置结构定义
type Config struct {
	Kafka KafkaConfig `json:"kafka" mapstructure:"kafka"`
	Nats  NatsConfig  `json:"nats" mapstructure:"nats"`
}

// Kafka配置
type KafkaConfig struct {
	Brokers       []string `json:"brokers" mapstructure:"brokers"`
	Topics        []string `json:"topics" mapstructure:"topics"`
	ConsumerGroup string   `json:"consumer_group" mapstructure:"consumer_group"`
	NumConsumers  int      `json:"num_consumers" mapstructure:"num_consumers"`
	EnableSASL    bool     `json:"enable_sasl" mapstructure:"enable_sasl"`
	SASLMechanism string   `json:"sasl_mechanism" mapstructure:"sasl_mechanism"`
	SASLUsername  string   `json:"sasl_username" mapstructure:"sasl_username"`
	SASLPassword  string   `json:"sasl_password" mapstructure:"sasl_password"`
	EnableTLS     bool     `json:"enable_tls" mapstructure:"enable_tls"`
}

// NATS配置
type NatsConfig struct {
	Port        int    `json:"port" mapstructure:"port"`
	Host        string `json:"host" mapstructure:"host"`
	UseEmbedded bool   `json:"use_embedded" mapstructure:"use_embedded"`
}

// 消息结构体，用于解析JSON
type Message struct {
	DomainID string `json:"domain_id"`
	// 其他需要的字段可以在这里添加
}

// 桥接器结构体
type KafkaNatsBridge struct {
	config     Config
	natsServer *server.Server // 仅用于嵌入式模式
	ctx        context.Context
	cancel     context.CancelFunc
}

// 消费者结构体，实现sarama.ConsumerGroupHandler接口
type Consumer struct {
	ready    chan bool
	id       int
	natsConn *nats.Conn
}

// Setup 在消费者开始消费前调用
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

// Cleanup 在消费者停止消费后调用
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim 处理消息的主要方法
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		// 解析消息中的JSON以获取domain_id
		var msg Message
		if err := json.Unmarshal(message.Value, &msg); err != nil {
			log.Printf("Consumer %d: Error unmarshaling message from topic %s: %v", consumer.id, message.Topic, err)
			session.MarkMessage(message, "")
			continue
		}

		// 构建NATS主题 {topic}-{domain_id}
		natsTopic := fmt.Sprintf("%s-%s", message.Topic, msg.DomainID)

		// 将原始消息发送到NATS
		if err := consumer.natsConn.Publish(natsTopic, message.Value); err != nil {
			log.Printf("Consumer %d: Error publishing to NATS topic %s: %v", consumer.id, natsTopic, err)
		} else {
			log.Printf("Consumer %d: Message forwarded to NATS, topic: %s, kafka topic: %s, partition: %d, offset: %d, domain_id: %s",
				consumer.id, natsTopic, message.Topic, message.Partition, message.Offset, msg.DomainID)
		}

		// 标记消息已处理
		session.MarkMessage(message, "")
	}
	return nil
}

// 初始化NATS服务器
func (bridge *KafkaNatsBridge) initNats() error {
	if bridge.config.Nats.UseEmbedded {
		ns, err := server.NewServer(&server.Options{
			Host: bridge.config.Nats.Host,
			Port: bridge.config.Nats.Port,
		})
		if err != nil {
			return fmt.Errorf("创建NATS服务器失败: %v", err)
		}

		bridge.natsServer = ns
		go ns.Start()

		if !ns.ReadyForConnections(5 * time.Second) {
			return fmt.Errorf("NATS服务器未能在预期时间内启动")
		}
		log.Printf("NATS嵌入式服务器已启动，监听端口: %d", bridge.config.Nats.Port)
	}
	return nil
}

// 获取新的NATS连接
func (bridge *KafkaNatsBridge) GetNatsConn() (*nats.Conn, error) {
	opts := []nats.Option{
		nats.Name(fmt.Sprintf("Consumer-%d", time.Now().UnixNano())),
		nats.ReconnectWait(500 * time.Millisecond),
		nats.MaxReconnects(-1),
		nats.DisconnectErrHandler(func(conn *nats.Conn, err error) {
			log.Printf("NATS连接断开: %v", err)
		}),
		nats.ReconnectHandler(func(conn *nats.Conn) {
			log.Printf("NATS重新连接到: %s", conn.ConnectedUrl())
		}),
	}

	if bridge.config.Nats.UseEmbedded {
		return nats.Connect("",
			append(opts, nats.InProcessServer(bridge.natsServer))...,
		)
	}
	return nats.Connect(fmt.Sprintf("nats://%s:%d", bridge.config.Nats.Host, bridge.config.Nats.Port), opts...)
}

// 配置Kafka客户端
func setupKafkaConfig(cfg KafkaConfig) *sarama.Config {
	config := sarama.NewConfig()

	// 配置消费者组
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true

	// 性能调优参数
	config.Consumer.Fetch.Min = 1                                 // 最小获取的字节数
	config.Consumer.Fetch.Default = 1024 * 1024                   // 默认获取的字节数（1MB）
	config.Consumer.MaxWaitTime = 500 * time.Second               // 最大等待时间（毫秒）
	config.ChannelBufferSize = 256                                // 通道缓冲大小
	config.Consumer.Offsets.AutoCommit.Enable = true              // 自动提交
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second // 提交间隔

	// 配置SASL认证
	if cfg.EnableSASL {
		config.Net.SASL.Enable = true
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		config.Net.SASL.User = cfg.SASLUsername
		config.Net.SASL.Password = cfg.SASLPassword

		// 如果是SCRAM认证，可以进一步配置
		if cfg.SASLMechanism == "SCRAM-SHA-256" {
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		} else if cfg.SASLMechanism == "SCRAM-SHA-512" {
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		}
	}

	// 配置TLS
	if cfg.EnableTLS {
		config.Net.TLS.Enable = true
		// 如果需要进一步配置TLS证书，可以在这里添加
	}

	// 版本设置
	config.Version = sarama.V2_0_0_0 // 使用适合您集群的版本

	return config
}

// 启动Kafka消费者
func (bridge *KafkaNatsBridge) startKafkaConsumers() error {
	// 创建Kafka配置
	config := setupKafkaConfig(bridge.config.Kafka)

	// 创建消费者组
	consumerGroup, err := sarama.NewConsumerGroup(bridge.config.Kafka.Brokers, bridge.config.Kafka.ConsumerGroup, config)
	if err != nil {
		return fmt.Errorf("创建消费者组失败: %v", err)
	}

	// 错误处理
	go func() {
		for err := range consumerGroup.Errors() {
			log.Printf("消费者组错误: %v", err)
		}
	}()

	// 创建等待组
	var wg sync.WaitGroup
	wg.Add(bridge.config.Kafka.NumConsumers)

	// 启动消费者
	for i := 0; i < bridge.config.Kafka.NumConsumers; i++ {
		go func(consumerID int) {
			defer wg.Done()

			nc, err := bridge.GetNatsConn()
			if err != nil {
				log.Printf("消费者 %d 创建NATS连接失败: %v", consumerID, err)
				return
			}
			defer nc.Close()

			consumer := Consumer{
				ready:    make(chan bool),
				id:       consumerID,
				natsConn: nc,
			}

			log.Printf("启动消费者 %d", consumerID)
			defer log.Printf("消费者 %d 已停止", consumerID)

			// 循环消费，直到接收到取消信号
			for {
				select {
				case <-bridge.ctx.Done():
					log.Printf("消费者 %d 接收到取消信号，退出", consumerID)
					return
				default:
					// 重置ready通道
					consumer.ready = make(chan bool)
					if err := consumerGroup.Consume(bridge.ctx, bridge.config.Kafka.Topics, &consumer); err != nil {
						log.Printf("消费者 %d 消费错误: %v", consumerID, err)
					}
					if bridge.ctx.Err() != nil {
						return
					}
				}
			}
		}(i)
	}

	// 注册清理函数
	go func() {
		<-bridge.ctx.Done()
		log.Println("关闭消费者组...")
		if err := consumerGroup.Close(); err != nil {
			log.Printf("关闭消费者组失败: %v", err)
		}
		wg.Wait()
		log.Println("所有消费者已退出")
	}()

	return nil
}

// 启动桥接器
func (bridge *KafkaNatsBridge) Start() error {
	// 初始化上下文
	bridge.ctx, bridge.cancel = context.WithCancel(context.Background())

	// 初始化NATS
	if err := bridge.initNats(); err != nil {
		return err
	}

	// 启动Kafka消费者
	if err := bridge.startKafkaConsumers(); err != nil {
		return err
	}

	log.Printf("Kafka-NATS桥接器已启动，连接到Kafka集群 %s，监听主题 %s",
		strings.Join(bridge.config.Kafka.Brokers, ","), strings.Join(bridge.config.Kafka.Topics, ","))

	return nil
}

// 停止桥接器
func (bridge *KafkaNatsBridge) Stop() {
	log.Println("停止桥接器...")

	// 取消上下文
	if bridge.cancel != nil {
		bridge.cancel()
	}

	if bridge.natsServer != nil {
		bridge.natsServer.Shutdown()
		log.Println("NATS嵌入式服务器已关闭")
	}
}

// 加载配置
func loadConfig() (Config, error) {
	// 设置默认配置
	defaultConfig := Config{
		Kafka: KafkaConfig{
			Brokers:       []string{"localhost:9092"},
			Topics:        []string{"test-topic"},
			ConsumerGroup: "kafka-nats-bridge",
			NumConsumers:  5,
			EnableSASL:    false,
			SASLMechanism: "PLAIN",
			SASLUsername:  "",
			SASLPassword:  "",
			EnableTLS:     false,
		},
		Nats: NatsConfig{
			Port:        4222,
			Host:        "localhost",
			UseEmbedded: true,
		},
	}

	// 初始化配置管理
	v := viper.New()

	// 设置默认值
	v.SetDefault("kafka", defaultConfig.Kafka)
	v.SetDefault("nats", defaultConfig.Nats)

	// 配置文件路径和名称
	v.SetConfigName("config")           // 配置文件名称（没有扩展名）
	v.SetConfigType("json")             // 配置文件类型
	v.AddConfigPath(".")                // 当前目录
	v.AddConfigPath("./config")         // config目录
	v.AddConfigPath("/etc/kafka-nats/") // 系统配置目录

	// 绑定命令行参数
	pflag.String("config", "", "配置文件路径")
	pflag.StringSlice("kafka.brokers", defaultConfig.Kafka.Brokers, "Kafka服务器地址列表")
	pflag.StringSlice("kafka.topics", defaultConfig.Kafka.Topics, "要消费的Kafka主题列表")
	pflag.String("kafka.consumer_group", defaultConfig.Kafka.ConsumerGroup, "Kafka消费者组ID")
	pflag.Int("kafka.num_consumers", defaultConfig.Kafka.NumConsumers, "Kafka消费者数量")
	pflag.Bool("kafka.enable_sasl", defaultConfig.Kafka.EnableSASL, "是否启用SASL认证")
	pflag.String("kafka.sasl_username", defaultConfig.Kafka.SASLUsername, "SASL用户名")
	pflag.String("kafka.sasl_password", defaultConfig.Kafka.SASLPassword, "SASL密码")
	pflag.String("nats.host", defaultConfig.Nats.Host, "NATS服务器主机")
	pflag.Int("nats.port", defaultConfig.Nats.Port, "NATS服务器端口")
	pflag.Bool("nats.use_embedded", defaultConfig.Nats.UseEmbedded, "是否使用嵌入式NATS服务器")
	pflag.Parse()

	// 将命令行参数绑定到viper
	v.BindPFlags(pflag.CommandLine)

	// 尝试读取指定的配置文件路径
	configFile := v.GetString("config")
	if configFile != "" {
		v.SetConfigFile(configFile)
	}

	// 尝试读取配置文件
	if err := v.ReadInConfig(); err != nil {
		// 如果配置文件未找到，但用户明确指定了配置文件，则返回错误
		if _, ok := err.(viper.ConfigFileNotFoundError); ok && configFile != "" {
			return defaultConfig, fmt.Errorf("找不到指定的配置文件: %s", configFile)
		}

		// 如果是其他错误，则返回错误
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return defaultConfig, fmt.Errorf("读取配置文件错误: %v", err)
		}

		log.Println("未找到配置文件，使用默认配置和命令行参数")
	} else {
		log.Printf("使用配置文件: %s", v.ConfigFileUsed())
	}

	// 使用环境变量覆盖配置（全部大写，使用_分隔）
	v.SetEnvPrefix("BRIDGE")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// 解析配置到结构体
	var config Config
	if err := v.Unmarshal(&config); err != nil {
		return defaultConfig, fmt.Errorf("无法解析配置: %v", err)
	}

	// 确保必需的配置项不为空
	if len(config.Kafka.Brokers) == 0 {
		return config, fmt.Errorf("kafka.brokers 不能为空")
	}

	if len(config.Kafka.Topics) == 0 {
		return config, fmt.Errorf("kafka.topics 不能为空")
	}

	if config.Kafka.ConsumerGroup == "" {
		return config, fmt.Errorf("kafka.consumer_group 不能为空")
	}

	return config, nil
}

// 生成默认配置文件
func generateDefaultConfig(filePath string) error {
	// 默认配置
	defaultConfig := Config{
		Kafka: KafkaConfig{
			Brokers:       []string{"localhost:9092"},
			Topics:        []string{"test-topic"},
			ConsumerGroup: "kafka-nats-bridge",
			NumConsumers:  5,
			EnableSASL:    false,
			SASLMechanism: "PLAIN",
			SASLUsername:  "",
			SASLPassword:  "",
			EnableTLS:     false,
		},
		Nats: NatsConfig{
			Port:        4222,
			Host:        "localhost",
			UseEmbedded: true,
		},
	}

	// 将配置转换为JSON
	jsonData, err := json.MarshalIndent(defaultConfig, "", "  ")
	if err != nil {
		return fmt.Errorf("无法序列化配置: %v", err)
	}

	// 写入文件
	if err := os.WriteFile(filePath, jsonData, 0644); err != nil {
		return fmt.Errorf("无法写入配置文件: %v", err)
	}

	log.Printf("已生成默认配置文件: %s", filePath)
	return nil
}

func main() {
	// 设置日志
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// 添加生成配置文件选项
	genConfig := pflag.Bool("gen-config", false, "生成默认配置文件")
	configPath := pflag.String("gen-config-path", "config.json", "生成的配置文件路径")
	pflag.Parse()

	// 如果用户要求生成配置文件
	if *genConfig {
		if err := generateDefaultConfig(*configPath); err != nil {
			log.Fatalf("生成配置文件失败: %v", err)
		}
		return
	}

	// 加载配置
	config, err := loadConfig()
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	// 创建桥接器
	bridge := &KafkaNatsBridge{
		config: config,
	}

	// 捕获系统信号
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// 启动桥接器
	if err := bridge.Start(); err != nil {
		log.Fatalf("启动桥接器失败: %v", err)
	}

	// 等待系统信号
	<-signals
	log.Println("接收到终止信号，开始优雅退出...")

	// 停止桥接器
	bridge.Stop()

	log.Println("程序已完全退出")
}
