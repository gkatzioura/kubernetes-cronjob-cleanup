/*
 * Copyright 2019 Emmanouil Gkatziouras
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"flag"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog"
	"kubernetes-cronjob-cleanup/pkg/controller"
	"kubernetes-cronjob-cleanup/pkg/signals"
)

var (
	masterURL  string
	kubeConfig string
)

func main() {
	flag.Parse()
	signalHandler := signals.SetupSignalHandler()

	config, err := clientcmd.BuildConfigFromFlags(masterURL, kubeConfig)

	if err != nil {
		klog.Fatalf("Error building kubeconfig: %s", err.Error())
	}

	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatalf("Error building kubernetes clientset: %s", err.Error())
	}

	controller.NewCleanUpController(kubeClient).Run(signalHandler)
}

func init() {
	flag.StringVar(&kubeConfig, "kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
	flag.StringVar(&masterURL, "master", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")
}
