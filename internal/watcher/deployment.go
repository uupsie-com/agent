package watcher

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/client-go/tools/cache"
)

func newDeploymentHandler(m *Manager) cache.ResourceEventHandlerFuncs {
	evaluate := func(obj interface{}) {
		deploy, ok := obj.(*appsv1.Deployment)
		if !ok {
			return
		}

		monitors := m.findMonitors("k8s_deployment", deploy.Namespace, deploy.Name)
		if len(monitors) == 0 {
			return
		}

		status, errMsg := evaluateDeployment(deploy)
		for _, mon := range monitors {
			m.reportStatus(mon.ID, status, errMsg)
		}
	}

	return cache.ResourceEventHandlerFuncs{
		AddFunc:    evaluate,
		UpdateFunc: func(_, newObj interface{}) { evaluate(newObj) },
		DeleteFunc: func(obj interface{}) {
			deploy, ok := obj.(*appsv1.Deployment)
			if !ok {
				return
			}
			monitors := m.findMonitors("k8s_deployment", deploy.Namespace, deploy.Name)
			msg := "deployment deleted"
			for _, mon := range monitors {
				m.reportStatus(mon.ID, "down", &msg)
			}
		},
	}
}

func evaluateDeployment(deploy *appsv1.Deployment) (string, *string) {
	desired := int32(1)
	if deploy.Spec.Replicas != nil {
		desired = *deploy.Spec.Replicas
	}

	available := deploy.Status.AvailableReplicas

	if available >= desired {
		return "up", nil
	}
	if available > 0 {
		msg := fmt.Sprintf("%d/%d replicas available", available, desired)
		return "degraded", &msg
	}
	msg := fmt.Sprintf("0/%d replicas available", desired)
	return "down", &msg
}
