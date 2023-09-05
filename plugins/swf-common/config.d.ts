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

export interface Config {
  /**
   * Configuration for the Serverless Workflow plugin.
   */
  swf: {
    baseUrl: string;
    port: string;
    workflowService: {
      path: string;
      container: string;
      owner?: string;
      environment?: string;
      jira?: {
        host?: string;
        /** @visibility secret */
        bearerToken?: string;
      };
    };
    /**
     * Editor-specific configuration.
     */
    editor: {
      /**
       * Path to the envelope (local or remote).
       * @visibility frontend
       */
      path: string;
    };
  };
}