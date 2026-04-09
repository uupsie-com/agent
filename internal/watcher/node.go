package watcher

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

func newNodeHandler(m *Manager) cache.ResourceEventHandlerFuncs {
	evaluate := func(obj interface{}) {
		node, ok := obj.(*corev1.Node)
		if !ok {
			return
		}

		monitors := m.findMonitors("k8s_node", "", node.Name)
		if len(monitors) == 0 {
			return
		}

		status, errMsg := evaluateNode(node)
		for _, mon := range monitors {
			m.reportStatus(mon.ID, status, errMsg)
		}
	}

	return cache.ResourceEventHandlerFuncs{
		AddFunc:    evaluate,
		UpdateFunc: func(_, newObj interface{}) { evaluate(newObj) },
		DeleteFunc: func(obj interface{}) {
			node, ok := obj.(*corev1.Node)
			if !ok {
				return
			}
			monitors := m.findMonitors("k8s_node", "", node.Name)
			msg := "node removed from cluster"
			for _, mon := range monitors {
				m.reportStatus(mon.ID, "down", &msg)
			}
		},
	}
}

func evaluateNode(node *corev1.Node) (string, *string) {
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady {
			if cond.Status == corev1.ConditionTrue {
				return "up", nil
			}
			msg := fmt.Sprintf("node not ready: %s", cond.Message)
			return "down", &msg
		}
	}
	msg := "node ready condition not found"
	return "down", &msg
}
