package watcher

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

func newServiceHandler(m *Manager) cache.ResourceEventHandlerFuncs {
	evaluate := func(obj interface{}) {
		svc, ok := obj.(*corev1.Service)
		if !ok {
			return
		}

		monitors := m.findMonitors("k8s_service", svc.Namespace, svc.Name)
		if len(monitors) == 0 {
			return
		}

		// For services we report "up" when the service exists.
		// Endpoint health is evaluated via the endpoints informer
		// which fires when backing pods change. For now, service
		// existence = up.
		for _, mon := range monitors {
			m.reportStatus(mon.ID, "up", nil)
		}
	}

	return cache.ResourceEventHandlerFuncs{
		AddFunc:    evaluate,
		UpdateFunc: func(_, newObj interface{}) { evaluate(newObj) },
		DeleteFunc: func(obj interface{}) {
			svc, ok := obj.(*corev1.Service)
			if !ok {
				return
			}
			monitors := m.findMonitors("k8s_service", svc.Namespace, svc.Name)
			msg := "service deleted"
			for _, mon := range monitors {
				m.reportStatus(mon.ID, "down", &msg)
			}
		},
	}
}
