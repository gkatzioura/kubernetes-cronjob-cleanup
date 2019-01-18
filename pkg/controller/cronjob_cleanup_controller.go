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

package controller

import (
	"fmt"
	batch_v1 "k8s.io/api/batch/v1"
	v13 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kube_runtime "k8s.io/apimachinery/pkg/runtime"
	util_runtime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"time"
)

type CleanUpController struct {
	kubeclient  kubernetes.Interface
	jobInformer cache.SharedIndexInformer
	jobQueue    workqueue.RateLimitingInterface
	recorder    record.EventRecorder
}

type ObjectMetaFetcher interface {
	ObjectMeta() meta_v1.ObjectMeta
}

func NewCleanUpController(kubeclient kubernetes.Interface) *CleanUpController {
	cleanUpController := &CleanUpController{
		kubeclient:  kubeclient,
		jobInformer: createJobInformer(kubeclient, v13.NamespaceAll, time.Second*30),
		jobQueue:    workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
	}

	addJobEventHandler(cleanUpController)

	return cleanUpController
}

func createJobInformer(kubeclient kubernetes.Interface, namespace string, resyncPeriod time.Duration) cache.SharedIndexInformer {
	listWatch := &cache.ListWatch{
		ListFunc: func(options meta_v1.ListOptions) (kube_runtime.Object, error) {
			println("options list func " + options.String())
			return kubeclient.BatchV1().Jobs(namespace).List(options)
		},
		WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
			println("options watch func " + options.String())
			return kubeclient.BatchV1().Jobs(namespace).Watch(options)
		},
	}

	podInformer := cache.NewSharedIndexInformer(
		listWatch,
		&batch_v1.Job{},
		resyncPeriod,
		cache.Indexers{},
	)

	return podInformer
}

func (cleanUpController *CleanUpController) Run(signalHandler <-chan struct{}) {
	defer util_runtime.HandleCrash()
	defer cleanUpController.jobQueue.ShutDown()

	go cleanUpController.jobInformer.Run(signalHandler)

	if !cache.WaitForCacheSync(signalHandler, cleanUpController.isSynced) {
		util_runtime.HandleError(fmt.Errorf("Could not sync cache"))
		return
	}

	wait.Until(cleanUpController.runWorker, time.Second, signalHandler)
}

func (cleanUpController *CleanUpController) isSynced() bool {
	return cleanUpController.jobInformer.HasSynced()
}

func (cleanUpController *CleanUpController) runWorker() {
	for cleanUpController.processItem() {
	}
}

func addJobEventHandler(cleanUpController *CleanUpController) {
	cleanUpController.jobInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			batchJob := obj.(*batch_v1.Job)
			ownerReference := obj.(*batch_v1.Job).GetObjectMeta().GetOwnerReferences()

			for i := 0; i < len(ownerReference); i++ {
				owner := ownerReference[i]

				if owner.Kind == "CronJob" {
					cleanUpController.deleteExtraChildJobs(batchJob.Namespace, owner.Name)
				}
			}
		},
		DeleteFunc: func(obj interface{}) {
			batchJob := obj.(*batch_v1.Job)
			println("Removed job ", batchJob.Name)
		},
	})
}

func (cleanUpController *CleanUpController) processItem() bool {

	key, quit := cleanUpController.jobQueue.Get()

	if quit {
		return false
	}

	defer cleanUpController.jobQueue.Done(key)

	// assert the string out of the key (format `namespace/name`)
	keyRaw := key.(string)

	_, exists, err := cleanUpController.jobInformer.GetIndexer().GetByKey(keyRaw)

	if err != nil {
		if cleanUpController.jobQueue.NumRequeues(key) < 5 {
			cleanUpController.jobQueue.AddRateLimited(key)
		} else {
			cleanUpController.jobQueue.Forget(key)
			util_runtime.HandleError(err)
		}
	}

	if !exists {
		cleanUpController.jobQueue.Forget(key)
	} else {
		cleanUpController.jobQueue.Forget(key)
	}

	return true
}

func (cleanUpController *CleanUpController) deleteExtraChildJobs(namespace string, cronjobName string) {

	//TODO Probably selector might give the options in the future to
	jobs, _ := cleanUpController.kubeclient.BatchV1().Jobs(namespace).List(meta_v1.ListOptions{})

	if len(jobs.Items) <= 1 {
		return
	}

	filtered := filterByOwner(jobs.Items, cronjobName)
	toDelete := filtered[:len(filtered)-1]

	for _, job := range toDelete {
		cleanUpController.deletePreviousJob(job)
	}
}

func filterByOwner(jobs []batch_v1.Job, owner string) []batch_v1.Job {
	filteredJobs := make([]batch_v1.Job, 0)
	for _, job := range jobs {
		if hasOwner(job, owner) {
			filteredJobs = append(filteredJobs, job)
		}
	}

	return filteredJobs
}

func hasOwner(job batch_v1.Job, owner string) bool {
	for _, ownerReference := range job.OwnerReferences {
		if ownerReference.Name == owner {
			return true
		}
	}

	return false
}

func (cleanUpController *CleanUpController) deletePreviousJob(job batch_v1.Job) {
	jobActions := cleanUpController.kubeclient.BatchV1().Jobs(job.Namespace)
	jobActions.Delete(job.Name, &meta_v1.DeleteOptions{})
}
