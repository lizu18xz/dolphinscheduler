/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { computed, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { useDeployMode, useResources, useCustomParams,useNamespace } from '.'
import type { IJsonItem } from '../types'

export function useSeaTunnel(model: { [field: string]: any }): IJsonItem[] {
  const { t } = useI18n()

  const configEditorSpan = computed(() => (model.useCustom ? 24 : 0))
  const resourceEditorSpan = computed(() => (model.useCustom ? 0 : 24))

  const flinkSpan = computed(() =>
    model.startupScript.includes('flink') ? 24 : 0
  )
  const deployModeSpan = computed(() =>
    model.startupScript.includes('spark') ||
    model.startupScript === 'seatunnel.sh'
      ? 24
      : 0
  )
  const masterSpan = computed(() =>
    model.startupScript.includes('spark') && model.deployMode !== 'local'
      ? 12
      : 0
  )
  const masterUrlSpan = computed(() =>
    model.startupScript.includes('spark') &&
    model.deployMode !== 'local' &&
    (model.master === 'SPARK' || model.master === 'MESOS')
      ? 12
      : 0
  )
  const showClient = computed(() => model.startupScript.includes('spark'))
  const showLocal = computed(() => model.startupScript === 'seatunnel.sh')
  const othersSpan = computed(() =>
    model.startupScript.includes('flink') ||
    model.startupScript === 'seatunnel.sh'
      ? 24
      : 0
  )

  return [

    useNamespace(),

    {
      type: 'input',
      field: 'image',
      name: t('project.node.image'),
      span: 18,
      props: {
        placeholder: t('project.node.image_tips')
      },
      validate: {
        trigger: ['input', 'blur'],
        required: false,
        message: t('project.node.image_tips')
      }
    },
    {
      type: 'select',
      field: 'imagePullPolicy',
      name: t('project.node.image_pull_policy'),
      span: 6,
      options: IMAGE_PULL_POLICY_LIST,
      validate: {
        trigger: ['input', 'blur'],
        required: false,
        message: t('project.node.image_pull_policy_tips')
      },
      value: 'IfNotPresent'
    },

    {
      type: 'input',
      field: 'jobManagerMemory',
      name: t('project.node.job_manager_memory'),
      span: 12,
      props: {
        placeholder: t('project.node.job_manager_memory_tips'),
        min: 1
      },
      validate: {
        trigger: ['input', 'blur'],
        validator(validate: any, value: string) {
          if (!value) {
            return
          }
          if (!Number.isInteger(parseInt(value))) {
            return new Error(
                t('project.node.job_manager_memory_tips') +
                t('project.node.positive_integer_tips')
            )
          }
        }
      }
    },
    {
      type: 'input',
      field: 'taskManagerMemory',
      name: t('project.node.task_manager_memory'),
      span: 12,
      props: {
        placeholder: t('project.node.task_manager_memory_tips')
      },
      validate: {
        trigger: ['input', 'blur'],
        validator(validate: any, value: string) {
          if (!value) {
            return
          }
          if (!Number.isInteger(parseInt(value))) {
            return new Error(
                t('project.node.task_manager_memory') +
                t('project.node.positive_integer_tips')
            )
          }
        }
      },
      value: model.taskManagerMemory
    },
    {
      type: 'input-number',
      field: 'slot',
      name: t('project.node.slot_number'),
      span: 12,
      props: {
        placeholder: t('project.node.slot_number_tips'),
        min: 1
      },
      value: model.slot
    },
    {
      type: 'input-number',
      field: 'taskManager',
      name: t('project.node.task_manager_number'),
      span: 12,
      props: {
        placeholder: t('project.node.task_manager_number_tips'),
        min: 1
      },
      value: model.taskManager
    },

    // SeaTunnel config parameter
    {
      type: 'switch',
      field: 'useCustom',
      name: t('project.node.custom_config')
    },
    {
      type: 'editor',
      field: 'rawScript',
      name: t('project.node.script'),
      span: configEditorSpan,
      validate: {
        trigger: ['input', 'trigger'],
        required: model.useCustom,
        validator(validate: any, value: string) {
          if (model.useCustom && !value) {
            return new Error(t('project.node.script_tips'))
          }
        }
      }
    },
    useResources(resourceEditorSpan, true, 1),
    ...useCustomParams({ model, field: 'localParams', isSimple: true })
  ]
}

export const IMAGE_PULL_POLICY_LIST = [
  {
    value: 'IfNotPresent',
    label: 'IfNotPresent'
  },
  {
    value: 'Always',
    label: 'Always'
  },
  {
    value: 'Never',
    label: 'Never'
  }
]

