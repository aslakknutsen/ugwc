package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayclient "sigs.k8s.io/gateway-api/pkg/client/clientset/versioned"
)

func main() {
	var (
		namespaces       int
		gwsPerNs         int
		routesPerGw      int
		nsPrefix         string
		gatewayClass     string
		listenerPort     int
		cleanupOnly      bool
		withServices     bool
		triggerEndpoint  bool
	)
	flag.IntVar(&namespaces, "namespaces", 1, "number of namespaces")
	flag.IntVar(&gwsPerNs, "gateways", 1, "gateways per namespace")
	flag.IntVar(&routesPerGw, "routes", 1, "httproutes per gateway")
	flag.StringVar(&nsPrefix, "ns-prefix", "perftest", "namespace name prefix")
	flag.StringVar(&gatewayClass, "gateway-class", "ugwc-conformance", "gatewayClassName")
	flag.IntVar(&listenerPort, "port", 8080, "base listener port")
	flag.BoolVar(&cleanupOnly, "cleanup", false, "only delete perftest namespaces, then exit")
	flag.BoolVar(&withServices, "services", true, "create a unique Service backend per gateway (required for endpoint fan-out tests)")
	flag.BoolVar(&triggerEndpoint, "trigger-endpoint", false, "after reconcile, patch one EndpointSlice to trigger targeted fan-out")
	flag.Parse()

	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, nil).ClientConfig()
	if err != nil {
		log.Fatalf("kubeconfig: %v", err)
	}

	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("kubernetes client: %v", err)
	}
	gwc, err := gatewayclient.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("gateway client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	nsNames := make([]string, namespaces)
	for i := range namespaces {
		nsNames[i] = fmt.Sprintf("%s-%d", nsPrefix, i)
	}

	if cleanupOnly {
		cleanup(ctx, kube, nsNames)
		return
	}

	type readyEvent struct {
		Kind      string
		Namespace string
		Name      string
		Created   time.Time
		Ready     time.Time
	}
	var (
		mu      sync.Mutex
		results []readyEvent
		pending sync.WaitGroup
	)
	created := make(map[string]time.Time)

	recordCreate := func(kind, ns, name string, t time.Time) {
		mu.Lock()
		defer mu.Unlock()
		created[kind+"/"+ns+"/"+name] = t
	}
	recordReady := func(kind, ns, name string, t time.Time) {
		mu.Lock()
		defer mu.Unlock()
		key := kind + "/" + ns + "/" + name
		if c, ok := created[key]; ok {
			results = append(results, readyEvent{kind, ns, name, c, t})
			delete(created, key)
			pending.Done()
		}
	}

	log.Println("starting watches...")

	gwWatcher, err := gwc.GatewayV1().Gateways("").Watch(ctx, metav1.ListOptions{})
	if err != nil {
		log.Fatalf("gateway watch: %v", err)
	}
	defer gwWatcher.Stop()

	routeWatcher, err := gwc.GatewayV1().HTTPRoutes("").Watch(ctx, metav1.ListOptions{})
	if err != nil {
		log.Fatalf("httproute watch: %v", err)
	}
	defer routeWatcher.Stop()

	go func() {
		for ev := range gwWatcher.ResultChan() {
			if ev.Type != watch.Modified {
				continue
			}
			gw, ok := ev.Object.(*gatewayv1.Gateway)
			if !ok {
				continue
			}
			if gatewayProgrammed(gw) {
				recordReady("Gateway", gw.Namespace, gw.Name, time.Now())
			}
		}
	}()

	go func() {
		for ev := range routeWatcher.ResultChan() {
			if ev.Type != watch.Modified {
				continue
			}
			rt, ok := ev.Object.(*gatewayv1.HTTPRoute)
			if !ok {
				continue
			}
			if httpRouteAccepted(rt) {
				recordReady("HTTPRoute", rt.Namespace, rt.Name, time.Now())
			}
		}
	}()

	for _, ns := range nsNames {
		_, err := kube.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: ns},
		}, metav1.CreateOptions{})
		if err != nil {
			log.Fatalf("create ns %s: %v", ns, err)
		}
	}
	log.Printf("created %d namespaces", namespaces)

	totalGw := namespaces * gwsPerNs
	totalRoutes := totalGw * routesPerGw
	pending.Add(totalGw + totalRoutes)

	for _, ns := range nsNames {
		for g := range gwsPerNs {
			gwName := fmt.Sprintf("gw-%d", g)
			svcName := fmt.Sprintf("svc-%d", g)

			if withServices {
				if err := createBackend(ctx, kube, ns, svcName); err != nil {
					log.Fatalf("create backend %s/%s: %v", ns, svcName, err)
				}
			}

			port := gatewayv1.PortNumber(int32(listenerPort + g))
			hostname := gatewayv1.Hostname(fmt.Sprintf("*.%s-%s.example.com", gwName, ns))
			gw := &gatewayv1.Gateway{
				ObjectMeta: metav1.ObjectMeta{Name: gwName, Namespace: ns},
				Spec: gatewayv1.GatewaySpec{
					GatewayClassName: gatewayv1.ObjectName(gatewayClass),
					Listeners: []gatewayv1.Listener{{
						Name:     "http",
						Port:     port,
						Protocol: gatewayv1.HTTPProtocolType,
						Hostname: &hostname,
					}},
				},
			}
			now := time.Now()
			if _, err := gwc.GatewayV1().Gateways(ns).Create(ctx, gw, metav1.CreateOptions{}); err != nil {
				log.Fatalf("create gw %s/%s: %v", ns, gwName, err)
			}
			recordCreate("Gateway", ns, gwName, now)
		}
	}
	log.Printf("created %d gateways", totalGw)

	for _, ns := range nsNames {
		for g := range gwsPerNs {
			gwName := fmt.Sprintf("gw-%d", g)
			svcName := fmt.Sprintf("svc-%d", g)
			gwObjName := gatewayv1.ObjectName(gwName)
			group := gatewayv1.Group("gateway.networking.k8s.io")
			kind := gatewayv1.Kind("Gateway")
			svcPort := gatewayv1.PortNumber(8080)
			for r := range routesPerGw {
				rtName := fmt.Sprintf("%s-rt-%d", gwName, r)
				hostname := gatewayv1.Hostname(fmt.Sprintf("%s.%s-%s.example.com", rtName, gwName, ns))
				rule := gatewayv1.HTTPRouteRule{
					Matches: []gatewayv1.HTTPRouteMatch{{
						Path: &gatewayv1.HTTPPathMatch{
							Type:  ptrTo(gatewayv1.PathMatchPathPrefix),
							Value: ptrTo("/"),
						},
					}},
				}
				if withServices {
					rule.BackendRefs = []gatewayv1.HTTPBackendRef{{
						BackendRef: gatewayv1.BackendRef{
							BackendObjectReference: gatewayv1.BackendObjectReference{
								Name: gatewayv1.ObjectName(svcName),
								Port: &svcPort,
							},
						},
					}}
				}
				rt := &gatewayv1.HTTPRoute{
					ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: ns},
					Spec: gatewayv1.HTTPRouteSpec{
						CommonRouteSpec: gatewayv1.CommonRouteSpec{
							ParentRefs: []gatewayv1.ParentReference{{
								Group: &group,
								Kind:  &kind,
								Name:  gwObjName,
							}},
						},
						Hostnames: []gatewayv1.Hostname{hostname},
						Rules:     []gatewayv1.HTTPRouteRule{rule},
					},
				}
				now := time.Now()
				if _, err := gwc.GatewayV1().HTTPRoutes(ns).Create(ctx, rt, metav1.CreateOptions{}); err != nil {
					log.Fatalf("create httproute %s/%s: %v", ns, rtName, err)
				}
				recordCreate("HTTPRoute", ns, rtName, now)
			}
		}
	}
	log.Printf("created %d httproutes", totalRoutes)
	log.Println("waiting for all resources to be reconciled...")

	done := make(chan struct{})
	go func() { pending.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		log.Println("timed out waiting for reconciliation")
	}

	if triggerEndpoint && withServices && len(nsNames) > 0 {
		triggerNS := nsNames[0]
		svcName := "svc-0"
		if err := patchEndpointSlice(ctx, kube, triggerNS, svcName); err != nil {
			log.Printf("trigger endpoint patch failed: %v", err)
		} else {
			log.Printf("patched EndpointSlice for %s/%s — check ugwc_index_fanout_size{event_kind=\"endpointslice\"}", triggerNS, svcName)
		}
	}

	mu.Lock()
	defer mu.Unlock()

	fmt.Println()
	fmt.Println("=== Results ===")
	fmt.Printf("%-10s %-30s %-30s %s\n", "KIND", "NAMESPACE/NAME", "CREATED", "LATENCY")

	var totalGwLatency, totalRtLatency time.Duration
	var gwCount, rtCount int
	for _, r := range results {
		lat := r.Ready.Sub(r.Created)
		fmt.Printf("%-10s %-30s %-30s %s\n", r.Kind, r.Namespace+"/"+r.Name, r.Created.Format(time.RFC3339), lat)
		if r.Kind == "Gateway" {
			totalGwLatency += lat
			gwCount++
		} else {
			totalRtLatency += lat
			rtCount++
		}
	}

	fmt.Println()
	if gwCount > 0 {
		fmt.Printf("Gateway    avg=%v  count=%d/%d\n", totalGwLatency/time.Duration(gwCount), gwCount, totalGw)
	}
	if rtCount > 0 {
		fmt.Printf("HTTPRoute  avg=%v  count=%d/%d\n", totalRtLatency/time.Duration(rtCount), rtCount, totalRoutes)
	}
	still := len(created)
	if still > 0 {
		fmt.Printf("\n%d resources never reconciled:\n", still)
		for k := range created {
			fmt.Printf("  %s\n", k)
		}
	}
}

func createBackend(ctx context.Context, kube kubernetes.Interface, ns, svcName string) error {
	_, err := kube.CoreV1().Services(ns).Create(ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: svcName, Namespace: ns},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": svcName},
			Ports: []corev1.ServicePort{{
				Name: "http",
				Port: 8080,
			}},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	_, err = kube.CoreV1().Pods(ns).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: ns,
			Labels:    map[string]string{"app": svcName},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "pause",
				Image: "registry.k8s.io/pause:3.9",
			}},
		},
	}, metav1.CreateOptions{})
	return err
}

func patchEndpointSlice(ctx context.Context, kube kubernetes.Interface, ns, svcName string) error {
	slices, err := kube.DiscoveryV1().EndpointSlices(ns).List(ctx, metav1.ListOptions{
		LabelSelector: "kubernetes.io/service-name=" + svcName,
	})
	if err != nil {
		return err
	}
	if len(slices.Items) == 0 {
		return fmt.Errorf("no EndpointSlice for service %s/%s", ns, svcName)
	}
	es := slices.Items[0]
	patch := []byte(`{"metadata":{"annotations":{"perftest-trigger":"` + time.Now().Format(time.RFC3339Nano) + `"}}}`)
	_, err = kube.DiscoveryV1().EndpointSlices(ns).Patch(
		ctx,
		es.Name,
		types.MergePatchType,
		patch,
		metav1.PatchOptions{},
	)
	return err
}

func gatewayProgrammed(gw *gatewayv1.Gateway) bool {
	for _, c := range gw.Status.Conditions {
		if c.Type == string(gatewayv1.GatewayConditionProgrammed) && c.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

func httpRouteAccepted(rt *gatewayv1.HTTPRoute) bool {
	for _, p := range rt.Status.Parents {
		for _, c := range p.Conditions {
			if c.Type == string(gatewayv1.RouteConditionAccepted) && c.Status == metav1.ConditionTrue {
				return true
			}
		}
	}
	return false
}

func ptrTo[T any](v T) *T { return &v }

func cleanup(ctx context.Context, kube kubernetes.Interface, namespaces []string) {
	for _, ns := range namespaces {
		err := kube.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{})
		if err != nil {
			log.Printf("delete ns %s: %v", ns, err)
		} else {
			log.Printf("deleted ns %s", ns)
		}
	}
}
