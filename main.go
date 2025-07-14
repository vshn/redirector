package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
)

const (
	sentinelPort = "26379"
	masterName   = "mymaster"
	tlsDir       = "/etc/redis-tls"

	leaseDuration = 2 * time.Second
	renewDeadline = 1 * time.Second
	retryPeriod   = 500 * time.Millisecond
)

var (
	namespace         = os.Getenv("POD_NAMESPACE")
	client            *clientset.Clientset
	kubeconfig        = os.Getenv("KUBECONFIG")
	redisPasswordFile = os.Getenv("REDIS_PASSWORD_FILE")
	podCount          = os.Getenv("POD_COUNT")
)

func setRedisMaster(ctx context.Context) {

	// Get username from env
	username := os.Getenv("REDIS_USERNAME")
	if username == "" {
		log.Fatal("REDIS_USERNAME environment variable must be set")
	}

	// Load password from file
	passwordBytes, err := os.ReadFile(redisPasswordFile)
	if err != nil {
		log.Fatalf("Failed to read Redis password from %s: %v", redisPasswordFile, err)
	}
	password := strings.TrimSpace(string(passwordBytes))

	// Load TLS configuration
	redisTlsEnabled, err := strconv.ParseBool(os.Getenv("REDIS_TLS_ENABLED"))
	if err != nil {
		klog.Fatalf("Can't determine if redis tls is enabled: %v", err)
	}

	var tlsConfig *tls.Config
	if redisTlsEnabled {
		tlsConfig, err = loadTLSConfig()
		if err != nil {
			log.Fatalf("Failed to load TLS config: %v", err)
		}
	}

	var sentinelClient *redis.SentinelClient
	var sentinelAddr string

	// Start loop to periodically check master
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	klog.Info("Starting redis master monitoring")

	for {
		select {
		case <-ctx.Done():
			klog.Info("Leader context canceled, stopping master monitoring")
			return

		case <-ticker.C:
			if sentinelClient == nil {
				sentinelClient, sentinelAddr = connectToSentinel(tlsConfig, username, password)
				if sentinelClient == nil {
					klog.Warningf("No Sentinel connection could be established")
					continue
				}
			}

			masterInfo, err := sentinelClient.GetMasterAddrByName(ctx, masterName).Result()
			if err != nil || len(masterInfo) != 2 {
				klog.Warningf("Sentinel %s failed: %v. Reconnection to another Sentinel...", sentinelAddr, err)
				_ = sentinelClient.Close()
				sentinelClient = nil // trigger reconnect on the next loop
				continue
			}

			klog.Infof("Current Redis master: %s:%s (via sentinel %s)\n", masterInfo[0], masterInfo[1], sentinelAddr)
		}
	}
}

func getNewLock(lockName, podName, namespace string) *resourcelock.LeaseLock {
	return &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      lockName,
			Namespace: namespace,
		},
		Client: client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: podName,
		},
	}
}

func runLeaderElection(lock *resourcelock.LeaseLock, ctx context.Context, id string) {
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   leaseDuration,
		RenewDeadline:   renewDeadline,
		RetryPeriod:     retryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(c context.Context) {
				setRedisMaster(c)
			},
			OnStoppedLeading: func() {
				klog.Info("no longer the leader, staying inactive")
			},
			OnNewLeader: func(current_id string) {
				if current_id == id {
					klog.Info("still the leader!")
					return
				}
				klog.Infof("new leader is %s", current_id)
			},
		},
	})
}

func loadTLSConfig() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(tlsDir+"/tls.crt", tlsDir+"/tls.key")
	if err != nil {
		return nil, fmt.Errorf("loading client cert/key: %w", err)
	}

	caCert, err := os.ReadFile(tlsDir + "/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("reading CA cert: %w", err)
	}

	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to append CA cert to pool")
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

func connectToSentinel(tlsConfig *tls.Config, username, password string) (*redis.SentinelClient, string) {
	if namespace == "" {
		klog.Fatal("POD_NAMESPACE must be set")
	}
	for pod := range podCount {
		host := fmt.Sprintf("redis-node-%d.redis-headless.%s.svc.cluster.local", pod, namespace)
		addr := fmt.Sprintf("%s:%s", host, sentinelPort)
		klog.Infof("Connecting to sentinel %s", addr)
		client := redis.NewSentinelClient(&redis.Options{
			Addr:        addr,
			TLSConfig:   tlsConfig,
			Username:    username,
			Password:    password,
			DialTimeout: 2 * time.Second,
			ReadTimeout: 2 * time.Second,
		})

		// Basic ping test
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		if err := client.Ping(ctx).Err(); err != nil {
			klog.Warningf("Sentinel %s unreachable: %v", addr, err)
			_ = client.Close()
			continue
		}

		klog.Infof("Connected to Sentinel: %s", addr)
		return client, addr
	}

	return nil, ""
}

func main() {
	var (
		leaseLockName string
		podName       = os.Getenv("POD_NAME")
	)

	flag.StringVar(&leaseLockName, "lease-name", "", "Name of lease lock")
	flag.Parse()

	if leaseLockName == "" {
		klog.Fatal("missing lease-name flag")
	}

	var config *rest.Config
	var err error
	if kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		config, err = rest.InClusterConfig()
	}
	if err != nil {
		klog.Fatalf("failed to get kubeconfig: %v", err)
	}
	client = clientset.NewForConfigOrDie(config)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lock := getNewLock(leaseLockName, podName, namespace)
	runLeaderElection(lock, ctx, podName)

}
