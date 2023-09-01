/*
 * Copyright 2023 The Backstage Authors
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
import { createApiRef } from '@backstage/core-plugin-api';
import {
  ProcessInstance,
  SwfItem,
  SwfListResult,
  SwfSpecFile,
} from '@backstage/plugin-swf-common';

export interface SwfApi {
  getSwf(swfId: string): Promise<SwfItem>;

  listSwfs(): Promise<SwfListResult>;

  getInstances(): Promise<ProcessInstance[]>;

  getInstance(instanceId: string): Promise<ProcessInstance>;

  createWorkflowDefinition(uri: string, content?: string): Promise<SwfItem>;

  deleteWorkflowDefinition(swfId: string): Promise<any>;

  getSpecs(): Promise<SwfSpecFile[]>;
}

export const swfApiRef = createApiRef<SwfApi>({
  id: 'plugin.swf.api',
});
