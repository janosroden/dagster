// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { PipelineRunStatus } from "./../../types/globalTypes";

// ====================================================
// GraphQL fragment: RunActionMenuFragment
// ====================================================

export interface RunActionMenuFragment_tags {
  __typename: "PipelineTag";
  key: string;
  value: string;
}

export interface RunActionMenuFragment_repositoryOrigin_repositoryLocationMetadata {
  __typename: "RepositoryMetadata";
  key: string;
  value: string;
}

export interface RunActionMenuFragment_repositoryOrigin {
  __typename: "RepositoryOrigin";
  id: string;
  repositoryLocationName: string;
  repositoryName: string;
  repositoryLocationMetadata: RunActionMenuFragment_repositoryOrigin_repositoryLocationMetadata[];
}

export interface RunActionMenuFragment {
  __typename: "PipelineRun";
  id: string;
  runId: string;
  rootRunId: string | null;
  pipelineName: string;
  solidSelection: string[] | null;
  pipelineSnapshotId: string | null;
  mode: string;
  canTerminate: boolean;
  tags: RunActionMenuFragment_tags[];
  status: PipelineRunStatus;
  repositoryOrigin: RunActionMenuFragment_repositoryOrigin | null;
}
