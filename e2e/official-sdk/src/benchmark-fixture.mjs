export const TABLES = {
  organizations: 'organizations',
  memberships: 'memberships',
  projects: 'projects',
  tasks: 'tasks',
  comments: 'comments'
};

export const TARGET_ORG_ID = 'org-001';
export const TARGET_USER_ID = 'user-org-001-0001';
export const DEFAULT_PROFILE = 'large';
export const BENCHMARK_STREAMS = ['tasks'];

export const PROFILES = {
  smoke: {
    orgCount: 1,
    usersPerOrg: 8,
    projectsPerOrg: 6,
    tasksPerProject: 6,
    commentsPerTask: 1,
    iterations: 1,
    concurrentClients: 2,
    batchInsertCount: 8,
    batchUpdateCount: 8,
    batchDeleteCount: 4,
    timeoutMs: 60_000
  },
  medium: {
    orgCount: 2,
    usersPerOrg: 36,
    projectsPerOrg: 48,
    tasksPerProject: 18,
    commentsPerTask: 3,
    iterations: 2,
    concurrentClients: 3,
    batchInsertCount: 30,
    batchUpdateCount: 30,
    batchDeleteCount: 12,
    timeoutMs: 75_000
  },
  large: {
    orgCount: 3,
    usersPerOrg: 72,
    projectsPerOrg: 96,
    tasksPerProject: 28,
    commentsPerTask: 4,
    iterations: 3,
    concurrentClients: 4,
    batchInsertCount: 96,
    batchUpdateCount: 96,
    batchDeleteCount: 32,
    timeoutMs: 120_000
  },
  '100k': {
    orgCount: 1,
    usersPerOrg: 256,
    projectsPerOrg: 250,
    tasksPerProject: 400,
    commentsPerTask: 0,
    iterations: 5,
    concurrentClients: 4,
    batchInsertCount: 250,
    batchUpdateCount: 250,
    batchDeleteCount: 100,
    timeoutMs: 600_000
  },
  '250k': {
    orgCount: 1,
    usersPerOrg: 512,
    projectsPerOrg: 500,
    tasksPerProject: 500,
    commentsPerTask: 0,
    iterations: 5,
    concurrentClients: 6,
    batchInsertCount: 500,
    batchUpdateCount: 500,
    batchDeleteCount: 200,
    timeoutMs: 900_000
  },
  '1m': {
    orgCount: 1,
    usersPerOrg: 1024,
    projectsPerOrg: 1000,
    tasksPerProject: 1000,
    commentsPerTask: 0,
    iterations: 5,
    concurrentClients: 8,
    batchInsertCount: 1000,
    batchUpdateCount: 1000,
    batchDeleteCount: 400,
    timeoutMs: 1_800_000
  },
  '2m': {
    orgCount: 1,
    usersPerOrg: 2048,
    projectsPerOrg: 2000,
    tasksPerProject: 1000,
    commentsPerTask: 0,
    iterations: 3,
    concurrentClients: 8,
    batchInsertCount: 2000,
    batchUpdateCount: 2000,
    batchDeleteCount: 800,
    timeoutMs: 3_600_000
  },
  '5m': {
    orgCount: 1,
    usersPerOrg: 4096,
    projectsPerOrg: 2500,
    tasksPerProject: 2000,
    commentsPerTask: 0,
    iterations: 3,
    concurrentClients: 8,
    batchInsertCount: 2500,
    batchUpdateCount: 2500,
    batchDeleteCount: 1000,
    timeoutMs: 7_200_000
  }
};

export function resolveProfile(profileName = DEFAULT_PROFILE) {
  const profile = PROFILES[profileName];

  if (!profile) {
    throw new Error(`Unknown benchmark profile: ${profileName}`);
  }

  return { name: profileName, ...profile };
}

export function buildBenchmarkFixture(profileName = DEFAULT_PROFILE) {
  const profile = resolveProfile(profileName);
  const targetOrgNumber = 1;
  const targetOrgId = orgId(targetOrgNumber);
  const memberships = profile.orgCount * profile.usersPerOrg;
  const projects = profile.orgCount * profile.projectsPerOrg;
  const baseTasks = profile.orgCount * profile.projectsPerOrg * profile.tasksPerProject;
  const sentinelTasks = 2 + profile.batchDeleteCount;
  const tasks = baseTasks + sentinelTasks;
  const comments = profile.orgCount * profile.projectsPerOrg * profile.tasksPerProject * profile.commentsPerTask;

  const batchUpdateIds = [];
  for (let index = 1; index <= profile.batchUpdateCount; index += 1) {
    batchUpdateIds.push(indexToTaskId(targetOrgNumber, index, profile.tasksPerProject));
  }

  const batchDeleteRows = [];
  for (let index = 1; index <= profile.batchDeleteCount; index += 1) {
    batchDeleteRows.push(makeSentinelTaskRow('batch-delete', index, targetOrgId, profile));
  }

  return {
    profile,
    targetOrgId,
    targetUserId: TARGET_USER_ID,
    expectedCounts: {
      [TABLES.organizations]: 0,
      [TABLES.memberships]: 0,
      [TABLES.projects]: 0,
      [TABLES.tasks]: tasks,
      [TABLES.comments]: 0
    },
    ids: {
      primaryProjectId: projectId(targetOrgNumber, 1),
      updateTaskId: sentinelTaskId('update', 1),
      updateTaskOriginalTitle: sentinelTaskTitle('Update sentinel'),
      updateTaskUpdatedTitle: sentinelTaskTitle('Update sentinel changed'),
      deleteTaskId: sentinelTaskId('delete', 1),
      deleteTaskRow: makeSentinelTaskRow('delete', 1, targetOrgId, profile),
      batchUpdateIds,
      batchDeleteRows,
      insertTaskIdPrefix: 'task-runtime-insert',
      insertTaskTitle: sentinelTaskTitle('Runtime insert sentinel')
    }
  };
}

export function buildSyncRulesYaml({ includeMultiBucketStreams = false } = {}) {
  const multiBucketStreams = includeMultiBucketStreams
    ? `  tasks_by_project:
    query: SELECT id, org_id, project_id, title, status, priority, assignee_id, story_points, updated_at, summary FROM ${TABLES.tasks} WHERE project_id = subscription.parameter('project_id')
  tasks_by_org:
    query: SELECT id, org_id, project_id, title, status, priority, assignee_id, story_points, updated_at, summary FROM ${TABLES.tasks} WHERE org_id = subscription.parameter('org_id')
`
    : '';
  return `
config:
  edition: 3
streams:
  tasks:
    auto_subscribe: true
    query: SELECT id, org_id, project_id, title, status, priority, assignee_id, story_points, updated_at, summary FROM ${TABLES.tasks}
${multiBucketStreams}`.trimStart();
}

export function orgId(index) {
  return `org-${pad(index, 3)}`;
}

export function userId(orgNumber, index) {
  return `user-${orgId(orgNumber)}-${pad(index, 4)}`;
}

export function membershipId(orgNumber, index) {
  return `membership-${orgId(orgNumber)}-${pad(index, 4)}`;
}

export function projectId(orgNumber, index) {
  return `project-${orgId(orgNumber)}-${pad(index, 4)}`;
}

export function taskId(orgNumber, projectIndex, taskIndex) {
  return `task-${orgId(orgNumber)}-${pad(projectIndex, 4)}-${pad(taskIndex, 4)}`;
}

export function commentId(orgNumber, projectIndex, taskIndex, commentIndex) {
  return `comment-${orgId(orgNumber)}-${pad(projectIndex, 4)}-${pad(taskIndex, 4)}-${pad(commentIndex, 4)}`;
}

export function sentinelTaskId(kind, index) {
  return `task-sentinel-${kind}-${pad(index, 4)}`;
}

export function sentinelTaskTitle(base) {
  return `${base} benchmark row`;
}

export function makeSentinelTaskRow(kind, index, targetOrgId, profile) {
  const projectIndex = ((index - 1) % profile.projectsPerOrg) + 1;
  const orgNumber = Number(targetOrgId.split('-')[1]);
  const assigneeIndex = ((index - 1) % profile.usersPerOrg) + 1;

  return {
    id: sentinelTaskId(kind, index),
    org_id: targetOrgId,
    project_id: projectId(orgNumber, projectIndex),
    title: sentinelTaskTitle(
      kind === 'update' ? 'Update sentinel' : kind === 'delete' ? 'Delete sentinel' : `Batch delete ${index}`
    ),
    status: kind === 'update' ? 'todo' : 'backlog',
    priority: (index % 5) + 1,
    assignee_id: userId(orgNumber, assigneeIndex),
    story_points: (index % 8) + 1,
    owner_id: TARGET_USER_ID,
    updated_at: '2026-01-01T00:00:00Z',
    summary: `sentinel:${kind}:${index}`
  };
}

export function indexToTaskId(orgNumber, ordinal, tasksPerProject) {
  const projectIndex = Math.floor((ordinal - 1) / tasksPerProject) + 1;
  const taskIndex = ((ordinal - 1) % tasksPerProject) + 1;
  return taskId(orgNumber, projectIndex, taskIndex);
}

export function generatedTaskTitle(taskIdValue) {
  const [, , , projectPart, taskPart] = taskIdValue.split('-');
  return `Task ${Number(projectPart)}.${Number(taskPart)}`;
}

export function pad(value, size) {
  return String(value).padStart(size, '0');
}
