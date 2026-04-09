package watcher

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

func newPodHandler(m *Manager) cache.ResourceEventHandlerFuncs {
	evaluate := func(obj interface{}) {
		pod, ok := obj.(*corev1.Pod)
		if !ok {
			return
		}

		monitors := m.findMonitors("k8s_pod", pod.Namespace, pod.Name)
		if len(monitors) == 0 {
			return
		}

		status, errMsg := evaluatePod(pod)
		for _, mon := range monitors {
			m.reportStatus(mon.ID, status, errMsg)
		}
	}

	return cache.ResourceEventHandlerFuncs{
		AddFunc:    evaluate,
		UpdateFunc: func(_, newObj interface{}) { evaluate(newObj) },
		DeleteFunc: func(obj interface{}) {
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				return
			}
			monitors := m.findMonitors("k8s_pod", pod.Namespace, pod.Name)
			msg := "pod deleted"
			for _, mon := range monitors {
				m.reportStatus(mon.ID, "down", &msg)
			}
		},
	}
}

func evaluatePod(pod *corev1.Pod) (string, *string) {
	switch pod.Status.Phase {
	case corev1.PodRunning:
		// Check all containers are ready
		for _, cs := range pod.Status.ContainerStatuses {
			if !cs.Ready {
				msg := fmt.Sprintf("container %s not ready", cs.Name)
				return "down", &msg
			}
		}
		return "up", nil
	case corev1.PodSucceeded:
		return "up", nil
	case corev1.PodPending:
		msg := "pod pending"
		return "degraded", &msg
	default:
		msg := fmt.Sprintf("pod phase: %s", pod.Status.Phase)
		return "down", &msg
	}
}
