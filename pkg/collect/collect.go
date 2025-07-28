package collect

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/mem"

	metric "keti/ai-storage-metric-collector/pkg/proto"

	"k8s.io/client-go/kubernetes"
)

// Prometheus Gauge 선언
var (
	cpuUsage = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "node_cpu_usage_percent",
		Help: "CPU usage percent (전체 평균)",
	})
	memUsage = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "node_memory_usage_percent",
		Help: "Memory usage percent (전체 메모리)",
	})
	diskUsage = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "node_disk_usage_percent",
		Help: "Disk usage percent (root 마운트)",
	})
)

type MetricCollector struct {
	HostKubeClient *kubernetes.Clientset
	Interval       *time.Duration
	ClusterMetric  metric.MultiMetric
}

func NewMetricCollector() *MetricCollector {
	// Prometheus 메트릭 등록
	prometheus.MustRegister(cpuUsage)
	prometheus.MustRegister(memUsage)
	prometheus.MustRegister(diskUsage)
	return &MetricCollector{
		HostKubeClient: nil,
		Interval:       nil,
		ClusterMetric:  metric.MultiMetric{},
	}
}

// 자원 측정 및 Gauge 기록 goroutine
func recordMetrics(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// CPU 사용률
				percent, err := cpu.Percent(0, false)
				if err == nil && len(percent) > 0 {
					cpuUsage.Set(percent[0])
				}
				// 메모리 사용률
				vmStat, err := mem.VirtualMemory()
				if err == nil {
					memUsage.Set(vmStat.UsedPercent)
				}
				// 디스크 사용률
				diskStat, err := disk.Usage("/")
				if err == nil {
					diskUsage.Set(diskStat.UsedPercent)
				}
			}
		}
	}()
}

// 핸들러: promhttp.Handler 사용
func MetricsHandler() http.Handler {
	return promhttp.Handler()
}

// 서버 실행부에서 MetricsHandler 사용
func (m *MetricCollector) RunMetricCollector(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	// 값 주기적 측정
	recordMetrics(ctx)

	mux := http.NewServeMux()
	mux.Handle("/metrics", MetricsHandler())

	srv := &http.Server{
		Addr:    ":2112",
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		log.Println("Shutting down metrics server...")
		srv.Shutdown(context.Background())
	}()

	log.Println("[metric-collector] start /metrics server at :2112")
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("ListenAndServe: %v", err)
	}
}
