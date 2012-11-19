/*
 * Copyright (C) 2008  Distributed Computing System (DCS) Group, Computer
 * Science Department - University of Piemonte Orientale, Alessandria (Italy).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package it.unipmn.di.dcs.grid.middleware.ourgrid;

//import it.unipmn.di.dcs.grid.core.middleware.sched.ExecutionStatus;
import it.unipmn.di.dcs.grid.core.middleware.sched.IJobHandle;
import it.unipmn.di.dcs.grid.core.middleware.sched.ITaskHandle;
import it.unipmn.di.dcs.grid.core.middleware.sched.monitor.AbstractJobMonitor;
import it.unipmn.di.dcs.grid.core.middleware.sched.monitor.IJobMonitor;
import it.unipmn.di.dcs.grid.core.middleware.sched.monitor.IJobMonitorContext;
import it.unipmn.di.dcs.grid.core.middleware.sched.monitor.IJobMonitorEventDispatcher;
import it.unipmn.di.dcs.grid.core.middleware.sched.monitor.IJobMonitorEventInterceptor;
import it.unipmn.di.dcs.grid.core.middleware.sched.monitor.ITaskMonitorContext;
import it.unipmn.di.dcs.grid.core.middleware.sched.monitor.JobMonitorContext;
import it.unipmn.di.dcs.grid.core.middleware.sched.monitor.TaskMonitorContext;

import org.ourgrid.mygrid.scheduler.ExecutionStatus;
import org.ourgrid.mygrid.scheduler.jobmanager.JobEntry;
import org.ourgrid.mygrid.scheduler.jobmanager.TaskEntry;
import org.ourgrid.mygrid.ui.MyGridUIManager;
import org.ourgrid.mygrid.ui.UIManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class for monitoring the executing of a specific job.
 *
 * @author <a href="mailto:marco.guazzone@gmail.com">Marco Guazzone</a>
 */
public class OurGridJobMonitor extends AbstractJobMonitor
{
	/** Number of milliseconds the running monitor will sleep. */
	private static final long DEFAULT_SLEEP_TIME = 5 * 1000;

	/** The running monitor. */
	private Thread monitor;

	/**
	 * A constructor.
	 */
	public OurGridJobMonitor(IJobHandle job)
	{
		super( job );
	}

	/**
	 * A constructor.
	 */
	public OurGridJobMonitor(IJobHandle job, IJobMonitorEventDispatcher dispatcher)
	{
		super( job, dispatcher );
	}

	//@{ AbstractJobMonitor extensions

	@Override
	public void run()
	{
		if (
			this.monitor == null
			|| !this.monitor.isAlive()
		)
		{
			this.monitor = new Thread( this.new Runner() );
			this.monitor.start();
		}
	}

	//@} AbstractJobMonitor extensions

	//@{ class OurGridJobMonitor.Runner

	/**
	 * The worker part of a job monitor.
	 *
	 * @author Marco Guazzone, &lt;marco.guazzone@unipmn.it&gt;
	 */
	private final class Runner implements Runnable
	{
		//@{ Runnable implementation

		public void run()
		{
			int jid;
			boolean isInterrupted = false;
			IJobHandle jobHnd = OurGridJobMonitor.this.getJobHandle();
			JobMonitorContext jobCtx = new JobMonitorContext( jobHnd );
			int ntasks = jobHnd.getTasksNum();
			int countCompletes = 0;

			ExecutionStatus oldOGJobStatus = ExecutionStatus.UNSTARTED;

			Map<Integer,ExecutionStatus> oldOGTaskStatusMap = new HashMap<Integer,ExecutionStatus>();

			IJobMonitorEventDispatcher dispatcher = OurGridJobMonitor.this.getDispatcher();

			try
			{
				jid = Integer.parseInt(
					OurGridJobMonitor.this.getJobHandle().getId()
				);
			}
			catch (Exception e)
			{
				//Log.severe( "Error while decoding job identifier from job handle." );
				return;
			}

			// Dispatches StartMonitoring event
			dispatcher.dispatchStartMonitoring( jobCtx );

			while ( countCompletes < ntasks && !isInterrupted )
			{
				try
				{
					Thread.sleep( OurGridJobMonitor.DEFAULT_SLEEP_TIME );

					if ( dispatcher.getInterceptors().size() > 0 )
					{
						// Retrieves the OurGrid job
						JobEntry job = OurGridUtil.GetOurGridJob( jid );
						if ( oldOGJobStatus != job.getStatus() )
						{
							// Maps the OurGrid exec status to the DCS exec status
							//jobCtx.setOldJobStatus( OurGridUtil.ToDcsExecStatus( oldOGJobStatus ) );
							jobCtx.setCurJobStatus( OurGridUtil.ToDcsExecStatus( job.getStatus() ) );

							// Dispatches JobStatusChange event
							dispatcher.dispatchJobStatusChange( jobCtx );

							// Initializes/Update old job status
							oldOGJobStatus = job.getStatus();
						}

						// Retrieves the related tasks
						for (TaskEntry task : job.getTasks())
						{
							int tid = task.getId();

							ITaskHandle taskHnd = null;
							TaskMonitorContext taskCtx = null;

							taskHnd = new OurGridTaskHandle( tid, OurGridJobMonitor.this.getJobHandle() );
							taskCtx = new TaskMonitorContext( taskHnd, jobCtx );

							ExecutionStatus oldOGTaskStatus = ExecutionStatus.UNSTARTED;

							oldOGTaskStatus = oldOGTaskStatusMap.get( tid );

							if ( oldOGTaskStatus != task.getState() )
							{
								// Maps the OurGrid exec status to the DCS exec status
								//taskCtx.setOldTaskStatus( OurGridUtil.ToDcsExecStatus( oldOGTaskStatus ) );
								taskCtx.setCurTaskStatus( OurGridUtil.ToDcsExecStatus( task.getState() ) );

								// Dispatches TaskStatusChange event
								dispatcher.dispatchTaskStatusChange( taskCtx );

								oldOGTaskStatusMap.put( tid, task.getState() );

								if (
									taskCtx.curTaskStatus() == it.unipmn.di.dcs.grid.core.middleware.sched.ExecutionStatus.FINISHED
									|| taskCtx.curTaskStatus() == it.unipmn.di.dcs.grid.core.middleware.sched.ExecutionStatus.FAILED
									|| taskCtx.curTaskStatus() == it.unipmn.di.dcs.grid.core.middleware.sched.ExecutionStatus.ABORTED
								)
								{
									countCompletes++;
								}
							}
						}
					}
				}
				catch (InterruptedException ie)
				{
					// Defers the interruption for
					// finalizing the execution.
					isInterrupted = true;
				}
				catch (Exception e)
				{
					//Log.severe( "Error while executing the job monitor worker: " + e.getMessage() );
					e.printStackTrace();
					return;
				}
			}

			dispatcher.dispatchStopMonitoring( jobCtx );

			if ( isInterrupted )
			{
				Thread.currentThread().interrupt();
			}
		}

		//@} Runnable implementation
	}

	//@} class OurGridJobMonitor.Runner
}
