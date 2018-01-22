/*
Copyright (c) 2016-2017 Bitnami

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package trigger

import (
	kubelessApi "github.com/kubeless/kubeless/pkg/apis/kubeless/v1beta1"
	"github.com/kubeless/kubeless/pkg/utils"
	"github.com/robfig/cron"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var updateCmd = &cobra.Command{
	Use:   "update <trigger_name> FLAG",
	Short: "Update a trigger to Kubeless",
	Long:  `Update a trigger to Kubeless`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			logrus.Fatal("Need exactly one argument - trigger name")
		}
		triggerName := args[0]

		triggerHTTP, err := cmd.Flags().GetBool("trigger-http")
		if err != nil {
			logrus.Fatal(err)
		}

		schedule, err := cmd.Flags().GetString("schedule")
		if err != nil {
			logrus.Fatal(err)
		}

		if schedule != "" {
			if _, err := cron.ParseStandard(schedule); err != nil {
				logrus.Fatalf("Invalid value for --schedule. " + err.Error())
			}
		}

		ns, err := cmd.Flags().GetString("namespace")
		if err != nil {
			logrus.Fatal(err)
		}
		if ns == "" {
			ns = utils.GetDefaultNamespace()
		}

		topic, err := cmd.Flags().GetString("trigger-topic")
		if err != nil {
			logrus.Fatal(err)
		}

		functionName, err := cmd.Flags().GetString("function-name")
		if err != nil {
			logrus.Fatal(err)
		}
		_, err = utils.GetFunction(functionName, ns)
		if err != nil {
			logrus.Fatalf("Unable to find the function %s in the namespace %s. Received %s: ", functionName, ns, err)
		}

		trigger := kubelessApi.Trigger{}
		trigger.TypeMeta = metav1.TypeMeta{
			Kind:       "Trigger",
			APIVersion: "kubeless.io/v1beta1",
		}

		switch {
		case triggerHTTP:
			trigger.Spec.Type = "HTTP"
			trigger.Spec.Topic = ""
			trigger.Spec.Schedule = ""
			break
		case schedule != "":
			trigger.Spec.Type = "Scheduled"
			trigger.Spec.Schedule = schedule
			trigger.Spec.Topic = ""
			break
		case topic != "":
			trigger.Spec.Type = "PubSub"
			trigger.Spec.Topic = topic
			trigger.Spec.Schedule = ""
			break
		}

		trigger.Spec.FunctionName = functionName
		trigger.ObjectMeta = metav1.ObjectMeta{
			Name:      triggerName,
			Namespace: ns,
			Labels: map[string]string{
				"created-by": "kubeless",
			},
		}

		kubelessClient, err := utils.GetKubelessClientOutCluster()
		if err != nil {
			logrus.Fatal(err)
		}

		logrus.Infof("Updating trigger...")
		err = utils.UpdateTriggerResource(kubelessClient, &trigger)
		if err != nil {
			logrus.Fatalf("Failed to deploy %s. Received:\n%s", triggerName, err)
		}
		logrus.Infof("Trigger %s submitted for deployment", triggerName)
		logrus.Infof("Check the deployment status executing 'kubeless trigger ls %s'", triggerName)
	},
}

func init() {
	updateCmd.Flags().StringP("namespace", "", "", "Specify namespace for the function")
	updateCmd.Flags().StringP("trigger-topic", "", "", "Deploy a pubsub function to Kubeless")
	updateCmd.Flags().StringP("schedule", "", "", "Specify schedule in cron format for scheduled function")
	updateCmd.Flags().Bool("trigger-http", false, "Deploy a http-based function to Kubeless")
	updateCmd.Flags().StringP("function-name", "", "", "Name of the function to be associated with trigger")
	updateCmd.MarkFlagRequired("function-name")
}
