import {Context} from './context'
import {JobDetails} from './types'
import {waitFor} from './utils'

const getTaskName = (taskName?: string) => {
  switch (taskName) {
    case 'gh-validation':
      return 'Fetching Metadata'
    case 'parse-metadata-migration':
      return 'Parsing metadata and migrations'
    case 'apply-metadata':
      return 'Applying metadata'
    case 'apply-migration':
      return 'Applying migrations'
    case 'reload-metadata':
      return 'Refreshing metadata'
    case 'check-healthz':
      return 'Checking Project Health'
    default:
      return `unknown task: ${taskName}`;
  }
}

const getTaskStatus = (status: string) => {
  if (status === 'created') {
    return 'started'
  }
  return status
}

const taskStartTimes: Record<string, Date> = {};
const getJobStatus = async (jobId: string, context: Context) => {
  try {
    const resp = await context.client.query<JobDetails, {jobId: string}>({
      query: `
        query getJobStatus($jobId: uuid!) {
          jobs_by_pk(id: $jobId) {
            status
            tasks(order_by: { updated_at: asc }) {
              id
              name
              cloud
              region task_events(order_by: { updated_at: desc }, limit: 1) {
                event_type
                id
                error
                github_detail
              }
            }
          }
        }
      `,
      variables: {
        jobId
      }
    })
    if (!resp.jobs_by_pk) {
      return null;
    }
    const tasksCount = resp.jobs_by_pk?.tasks.length
    if (tasksCount && tasksCount > 0) {
      context.logger.log(JSON.stringify(resp.jobs_by_pk, null, 2));
      const latestTask = resp.jobs_by_pk?.tasks[tasksCount - 1]
      const taskEventsCount = latestTask?.task_events.length
      if (latestTask && taskEventsCount && taskEventsCount > 0) {
        const latestTaskEvent = latestTask.task_events[taskEventsCount - 1]

        const taskName = getTaskName(latestTask.name);
        const taskNameKey = `${latestTask.name}: ${taskName}`;
        if (!(taskNameKey in taskStartTimes)) {
          taskStartTimes[taskNameKey] = new Date();
        }

        context.logger.log(
          `${taskName}: ${getTaskStatus(
            latestTaskEvent?.event_type
          )}`,
          false
        )
        if (latestTaskEvent?.github_detail) {
          context.logger.log(latestTaskEvent?.github_detail, false)
        }
        if (
          latestTaskEvent &&
          latestTaskEvent.event_type === 'failed' &&
          latestTaskEvent.error
        ) {
          context.logger.log(latestTaskEvent?.error, false)
        }
      }
    }
    return resp.jobs_by_pk.status
  } catch (e) {
    if (e instanceof Error) {
      context.logger.log(e.message)
    }
    throw e
  }
}

const GET_JOB_STATUS_RETRIES = 3;
export const getRealtimeLogs = async (
  jobId: string,
  context: Context,
  retryCount = 0
) => {
  if (retryCount > 0) {
    await waitFor(2000);
  }
  let jobStatus: string | null = null;
  let getJobStatusTries = 1;
  while (!jobStatus) {
    jobStatus = await getJobStatus(jobId, context);
    if (jobStatus) {
      break;
    }
    if (getJobStatusTries > GET_JOB_STATUS_RETRIES) {
      throw new Error("get job failed to return a value");
    } else {
      context.logger.log("Job status for jobId not present, retrying...");
    }
    getJobStatusTries += 1;
    await waitFor(2000);
  }
  if (jobStatus === 'success') {
    context.logger.log(JSON.stringify({taskStartTimes, completionTime: new Date()}, null, 2));
    return 'success'
  }
  if (jobStatus === 'failed') {
    context.logger.log(JSON.stringify({taskStartTimes, completionTime: new Date()}, null, 2));
    return 'failed'
  }
  return getRealtimeLogs(jobId, context, retryCount + 1)
}
