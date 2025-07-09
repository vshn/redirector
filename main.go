package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog"
)

const (
	sentinelService = "redis-headless"
	sentinelPort    = "26379"
	masterName      = "mymaster"

	leaseDuration = 15 * time.Second
	renewDeadline = 10 * time.Second
	retryPeriod   = 2 * time.Second
)

var (
	namespace = os.Getenv("POD_NAMESPACE")
	client    *clientset.Clientset
)

func setRedisMaster(ctx context.Context) {

	sentinels := []string{
		fmt.Sprintf("%s:%s", sentinelService, sentinelPort),
	}

	sentinel := redis.NewSentinelClient(&redis.Options{
		Addr: sentinels[0],
	})

	masterInfo, err := sentinel.GetMasterAddrByName(ctx, masterName).Result()
	if err != nil {
		klog.Fatalf("Failed to get master address: %v", err)
	}

	if len(masterInfo) != 2 {
		klog.Fatalf("Unexpected master info: %v", masterInfo)
	}

	masterHost := masterInfo[0]
	masterPort := masterInfo[1]

	klog.Infof("Current Redis master is %s:%s\n", masterHost, masterPort)

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
				setRedisMaster(ctx)
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

	config, err := rest.InClusterConfig()
	client = clientset.NewForConfigOrDie(config)

	if err != nil {
		klog.Fatalf("failed to get kubeconfig: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lock := getNewLock(leaseLockName, podName, namespace)
	runLeaderElection(lock, ctx, podName)

}
