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

import it.unipmn.di.dcs.common.util.collection.Collections;
import it.unipmn.di.dcs.grid.core.format.jdf.JdfExporter;
import it.unipmn.di.dcs.grid.core.middleware.sched.IBotJob;
import it.unipmn.di.dcs.grid.core.middleware.sched.IBotTask;
import it.unipmn.di.dcs.grid.core.middleware.sched.ExecutionStatus;
import it.unipmn.di.dcs.grid.core.middleware.sched.IJob;
import it.unipmn.di.dcs.grid.core.middleware.sched.IJobHandle;
import it.unipmn.di.dcs.grid.core.middleware.sched.IJobRequirements;
import it.unipmn.di.dcs.grid.core.middleware.sched.IRemoteCommand;
import it.unipmn.di.dcs.grid.core.middleware.sched.ISchedulerDriver;
import it.unipmn.di.dcs.grid.core.middleware.sched.IStageInRule;
import it.unipmn.di.dcs.grid.core.middleware.sched.IStageOutRule;
import it.unipmn.di.dcs.grid.core.middleware.sched.monitor.IJobMonitor;
import it.unipmn.di.dcs.grid.core.middleware.sched.SchedulerException;
import it.unipmn.di.dcs.grid.middleware.ourgrid.wrapper.DescriptionFileCompileWrapper;

import java.io.File;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

//import org.ourgrid.mgconnector.common.ConnectorJob;
//import org.ourgrid.mgconnector.handler.MyGridHandler;
//import org.ourgrid.mgconnector.handler.MyGridHandlingException;
//import org.ourgrid.mgconnector.main.MgConnector;

//import org.ourgrid.mygrid.scheduler.PeerEntry;
//import org.ourgrid.mygrid.scheduler.exception.JobCouldNotBeCancelledException;
//import org.ourgrid.mygrid.scheduler.exception.JobNotFoundException;
//import org.ourgrid.mygrid.scheduler.jobmanager.JobEntry;
import org.ourgrid.common.config.Configuration;
import org.ourgrid.common.config.LogConfiguration;
import org.ourgrid.common.spec.JobSpec;
//import org.ourgrid.common.spec.main.CommonCompiler;
import org.ourgrid.mygrid.scheduler.jobmanager.JobEntry;
import org.ourgrid.mygrid.scheduler.jobmanager.ReplicaEntry;
import org.ourgrid.mygrid.scheduler.jobmanager.TaskEntry;
import org.ourgrid.mygrid.scheduler.ReplicaExecutorResult;
//import org.ourgrid.common.spec.main.DescriptionFileCompile;
import org.ourgrid.mygrid.ui.MyGridUIManager;
import org.ourgrid.mygrid.ui.UIManager;
import org.ourgrid.mygrid.ui.exception.CouldNotAddJobOutsideHomeMachineException;

/**
 * Scheduler driver for the OurGrid middleware.
 *
 * @author <a href="mailto:marco.guazzone@gmail.com">Marco Guazzone</a>
 */
public class OurGridSchedulerDriver implements ISchedulerDriver
{
//	private MyGridHandler mgh;

	private static boolean envPrepared = false;
	private static boolean logPrepared = false;

	private static final Pattern JobVarPattern = Pattern.compile( ".*\\$JOB.*" );
	private static final Pattern TaskVarPattern = Pattern.compile( ".*\\$TASK.*" );

	public OurGridSchedulerDriver()
	{
//		//TODO: handle MG connector properties!
//
//		this.mgh = MgConnector.createHandler();
	}

	private static void prepareEnv()
	{
		if ( OurGridSchedulerDriver.envPrepared )
		{
			return;
		}

		OurGridSchedulerEnv.GetInstance().initialize();

		//OurGridSchedulerDriver.envPrepared = true;
	}

        /**
	 * Prepares a log to be used. When configuring a log, it will create the
	 * necessary directories and also check for a log properties file to be used
	 * to make this configuration.
	 */
	private static void prepareLog()
	{
		if ( OurGridSchedulerDriver.logPrepared )
		{
			return;
		}

		String logFile = Configuration.getInstance( Configuration.MYGRID ).getLogPath();

		String parents = new File( logFile ).getParent();

		if ( parents != null )
		{
			(new File( parents )).mkdirs();
		}

		LogConfiguration.configure( Configuration.getInstance( Configuration.MYGRID ).getLogPropertiesPath(), logFile );

		OurGridSchedulerDriver.logPrepared = true;
	}

	//@{ ISchedulerDriver implementation

	public void abortJob(IJobHandle job) throws SchedulerException
	{
		if( !this.isRunning() )
		{
			throw new SchedulerException( "MyGrid is not running" );
		}               
                
		OurGridSchedulerDriver.prepareEnv();

		Configuration.getInstance( Configuration.MYGRID );

		OurGridSchedulerDriver.prepareLog();

		try
		{
			int jid = Integer.parseInt( job.getId() );

			UIManager services;

			services = MyGridUIManager.getInstance();

			// Aborts all running task replica.
			JobEntry mgjob = null;
			for (JobEntry entry : services.getJobs(null))
			{
				if ( entry.getId() == jid )
				{
					mgjob = entry;
					break;
				}
			}
			if ( mgjob == null )
			{
				throw new SchedulerException( "There is no job on the scheduler with id '" + jid + "'" );
			}
			//Collection<TaskEntry> tasks = mgjob.getAvailableTasks();
			for (TaskEntry task : mgjob.getTasks())
			{
				for (ReplicaEntry replica : task.getReplicas())
				{
					replica.setStatus( org.ourgrid.mygrid.scheduler.ExecutionStatus.ABORTED );
					task.replicaAborted( new ReplicaExecutorResult(replica) );
				}
			}

			//FIXME: actually MyGrid doesn't allow to abort a job.
			//       Abort is allowed only for task replica.
			//       A job can only be set to FAILED o CAONCELLED.
                        services.cancelJob( jid );

			services = null;
		}
		catch (Exception e)
		{
			throw new SchedulerException( "Error while aborting job '" + job.getId() + "'", e );
		}
	}

	public void cancelJob(IJobHandle job) throws SchedulerException
	{
		if( !this.isRunning() )
		{
			throw new SchedulerException( "MyGrid is not running" );
		}               
                
		OurGridSchedulerDriver.prepareEnv();

		Configuration.getInstance( Configuration.MYGRID );

		OurGridSchedulerDriver.prepareLog();

		try
		{
			int jid = Integer.parseInt( job.getId() );

			UIManager services;

			services = MyGridUIManager.getInstance();

			services.cancelJob( jid ); 

			services = null;
		}
		catch (Exception e)
		{
			throw new SchedulerException( "Error while cancelling job '" + job.getId() + "'", e );
		}
	}

	public String createTaskUniqueFileName(String fileNamePrefix) throws SchedulerException
	{
		// Checks if the file name need a change 
		if ( TaskVarPattern.matcher( fileNamePrefix ).matches() )
		{
			// no change is needed
			return fileNamePrefix;
		}

		return fileNamePrefix + ".$JOB-$TASK";
	}

	public boolean existsJob(String jobId) throws SchedulerException
	{
		boolean exists = false;

		try
		{
			int jid = Integer.parseInt( jobId );

			OurGridSchedulerDriver.prepareEnv();

			Configuration.getInstance( Configuration.MYGRID );

			OurGridSchedulerDriver.prepareLog();

			UIManager services = MyGridUIManager.getInstance();

			Collection<JobEntry> myGridJobs = null;
			myGridJobs = services.getJobs( null );

			services = null;

			JobEntry mgjob = null;
			for (JobEntry entry : myGridJobs)
			{
				if ( entry.getId() == jid )
				{
					mgjob = entry;
					break;
				}
			}
		   
			if ( mgjob != null )
			{
				exists = true;
			}
		}
		catch (Exception e)
		{
			throw new SchedulerException( "Error while getting information for job '" + jobId + "'", e );
		}

		return exists;
	}

	public IJobHandle getJobHandle(String jobId) throws SchedulerException
	{
		// Checks the scheduler
		if( !this.isRunning() )
		{
			throw new SchedulerException( "MyGrid is not running" );
		}               

		IJobHandle jhnd = null;

		try
		{
			//// Get the job id
			//int jid = Integer.parseInt( jobId );
			//
			//// Creates the handle
			//jhnd = new OurGridJobHandle( jid );
			jhnd = OurGridSchedulerDriver.JobIdToJobHandle( jobId );

			// Checks if the job exists
			this.getJobStatus( jhnd );
		}
		catch (Exception e)
		{
			throw new SchedulerException( "There is no job on the scheduler with id '" + jobId + "'", e );
		}

		return jhnd;
	}

	public IJobMonitor getJobMonitor(IJobHandle job) throws SchedulerException
	{
		return new OurGridJobMonitor( job );
	}

	public String createJobUniqueFileName(String fileNamePrefix) throws SchedulerException
	{
		// Checks if the file name need a change 
		if ( JobVarPattern.matcher( fileNamePrefix ).matches() )
		{
			// no change is needed
			return fileNamePrefix;
		}

		return fileNamePrefix + ".$JOB";
	}

	public ExecutionStatus getJobStatus(IJobHandle job) throws SchedulerException
	{
		ExecutionStatus status = null;

		try
		{
			int jid = Integer.parseInt( job.getId() );

//			ConnectorJob mgjob = this.mgh.getJobById( jid );

			OurGridSchedulerDriver.prepareEnv();

			Configuration.getInstance( Configuration.MYGRID );

			OurGridSchedulerDriver.prepareLog();

			UIManager services = MyGridUIManager.getInstance();

			Collection<JobEntry> myGridJobs = null;
			myGridJobs = services.getJobs( null );

			services = null;

			JobEntry mgjob = null;
			for (JobEntry entry : myGridJobs)
			{
				if ( entry.getId() == jid )
				{
					mgjob = entry;
					break;
				}
			}
		   
			if ( mgjob == null )
			{
				throw new SchedulerException( "There is no job on the scheduler with id '" + jid + "'" );
			}

			status = OurGridUtil.ToDcsExecStatus( mgjob.getStatus() );
		}
		catch (SchedulerException me)
		{
			throw me;
		}
		catch (Exception e)
		{
			throw new SchedulerException( "Error while getting status of job '" + job.getId() + "'", e );
		}

		return status;
	}

	public boolean isRunning()
	{
		boolean ret = false;

		try
		{
			OurGridSchedulerDriver.prepareEnv();

			Configuration.getInstance( Configuration.MYGRID );

			OurGridSchedulerDriver.prepareLog();

			UIManager services = MyGridUIManager.getInstance();

			ret = services.isMyGridRunning();

			services = null;
		}
		catch (Exception e)
		{
			// empty
		}

		return ret;
	}

	public void purgeJob(IJobHandle job) throws SchedulerException
	{
		if( !this.isRunning() )
		{
			throw new SchedulerException( "MyGrid is not running" );
		}               
                
		OurGridSchedulerDriver.prepareEnv();

		Configuration.getInstance( Configuration.MYGRID );

		OurGridSchedulerDriver.prepareLog();

		try
		{
			int jid = Integer.parseInt( job.getId() );

			UIManager services;

			services = MyGridUIManager.getInstance();

			services.removeJob( jid ); 

			services = null;
		}
		catch (Exception e)
		{
			throw new SchedulerException( "Error while purging job '" + job.getId() + "'", e );
		}
	}

	public IJobHandle submitJob(IJob job) throws SchedulerException
	{
/*
		ConnectorJob mgJob = null;
		int jid;

                if( !this.isRunning() )
		{
			throw new SchedulerException( "MyGrid is not running" );
                }               

		try
		{
			mgJob = OurGridSchedulerDriver.CreateConnectorJob( job );

			jid = this.mgh.addJob( mgJob );
		}
		catch(Exception e)
		{
			throw new SchedulerException( "Error while submitting job '" + job.getName() + "': " + e );
		}

		return new OurGridJobHandle( jid );
*/
//		ConnectorJob mgJob = null;
		int jid;

                if( !this.isRunning() )
		{
			throw new SchedulerException( "MyGrid is not running" );
                }               

		try
		{
//			mgJob = OurGridSchedulerDriver.CreateConnectorJob( job );

//			jid = this.mgh.addJob( mgJob );

			OurGridSchedulerDriver.prepareEnv();

			Configuration.getInstance( Configuration.MYGRID );

			OurGridSchedulerDriver.prepareLog();

			UIManager services = MyGridUIManager.getInstance();

			File jdfTmpFile = File.createTempFile( "tmp", "jdf" );
			jdfTmpFile.deleteOnExit();
			PrintWriter pw = new PrintWriter( jdfTmpFile );
			JdfExporter jdfExp = new JdfExporter();
			jdfExp.export( job, pw );
			pw.close();
			pw = null;

			JobSpec jobSpec = null;

			//jobSpec = DescriptionFileCompile.compileJDF( jdfTmpFile.getAbsolutePath() );
			jobSpec = DescriptionFileCompileWrapper.compileJDF( jdfTmpFile.getAbsolutePath() );

			jid = services.addJob( jobSpec );

			services = null;
		}
		catch(Exception e)
		{
			throw new SchedulerException( "Error while submitting job '" + job.getName() + "'", e );
		}

		return new OurGridJobHandle( jid );
	}

	public IJobHandle submitJob(IBotJob job) throws SchedulerException
	{
		//return this.submitJob( (IJob) job ); //DON'T WORK

		int jid;

                if( !this.isRunning() )
		{
			throw new SchedulerException( "MyGrid is not running" );
                }               

		try
		{
//			mgJob = OurGridSchedulerDriver.CreateConnectorJob( job );

//			jid = this.mgh.addJob( mgJob );

			OurGridSchedulerDriver.prepareEnv();

			Configuration.getInstance( Configuration.MYGRID );

			OurGridSchedulerDriver.prepareLog();

			UIManager services = MyGridUIManager.getInstance();

			File jdfTmpFile = File.createTempFile( "tmp", "jdf" );
			jdfTmpFile.deleteOnExit();
			PrintWriter pw = new PrintWriter( jdfTmpFile );
			JdfExporter jdfExp = new JdfExporter();
			jdfExp.export( job, pw );
			pw.close();
			pw = null;

			JobSpec jobSpec = null;

			//jobSpec = DescriptionFileCompile.compileJDF( jdfTmpFile.getAbsolutePath() );
			jobSpec = DescriptionFileCompileWrapper.compileJDF( jdfTmpFile.getAbsolutePath() );

			jid = services.addJob( jobSpec );

			services = null;
		}
		catch(Exception e)
		{
			throw new SchedulerException( "Error while submitting job '" + job.getName() + "'", e );
		}

		return new OurGridJobHandle( jid );
	}

	//@} ISchedulerDriver implementation

	/** Returns a IJobHandle object from a job identifier. */
	protected static IJobHandle JobIdToJobHandle(String jobId)
	{
		// Get the job id
		int jid = Integer.parseInt( jobId );

		// Creates the handle
		return new OurGridJobHandle( jid );
	}

/*
	@Deprecated
	protected static ConnectorJob CreateConnectorJob(IJob job)
	{
		ConnectorJob mgJob = null;

		//StringBuilder jobReqs = new StringBuilder();
		//if ( job.getRequirements() != null )
		//{
		//	for ( IJobRequirements req : job.getRequirements() )
		//	{
		//		if ( jobReqs.length() > 0 )
		//		{
		//			jobReqs.append( " and " );
		//		}
		//		jobReqs.append( "(" + req.toJdf() + ")" );
		//	}
		//}
		//mgJob = new ConnectorJob( jobReqs.toString(), job.getName() );
		//jobReqs = null;

		mgJob = new ConnectorJob(
			( job.getRequirements() != null ) ? job.getRequirements().toString() : null,
			job.getName()
		);

		if (
			job.getStageIn() != null
			|| job.getCommands() != null
			|| job.getStageOut() != null
		)
		{
			String[] inputs = null;
			StringBuilder commands = null;;
			String[] outputs = null;

			// Transform a StageIn object to an array of "stringified" stage-in rules
			if ( job.getStageIn() != null && job.getStageIn().getRules() != null )
			{
				inputs = new String[ job.getStageIn().getRules().size() ];

				int i = 0;
				for (IStageInRule rule : job.getStageIn().getRules() )
				{
					inputs[ i++ ] = rule.toString();
				}
			}

			// Transform a list of RemoteCommands to a single string.
			if ( job.getCommands() != null )
			{
				for ( IRemoteCommand com : job.getCommands() )
				{
					if ( commands.length() > 0 )
					{
						commands.append( OurGridEnv.GetInstance().getCommandSeparator() );
					}
					commands.append( com.toString() );
				}
			}


			// Transform a StageOut object to an array of "stringified" stage-out rules
			if ( job.getStageOut() != null && job.getStageOut().getRules() != null )
			{
				outputs = new String[ job.getStageOut().getRules().size() ];

				int i = 0;
				for (IStageOutRule rule : job.getStageOut().getRules() )
				{
					outputs[ i++ ] = rule.toString();
				}
			}

			mgJob.addConnectorTask(
				inputs,
				( commands != null ) ? commands.toString() : null,
				outputs
			);
		}

		return mgJob;
	}

	@Deprecated
	protected static ConnectorJob CreateConnectorJob(IBotJob job)
	{
		ConnectorJob mgJob = null;

		//StringBuilder jobReqs = new StringBuilder();
		//if ( job.getRequirements() != null )
		//{
		//	for ( IJobRequirement req : job.getRequirements() )
		//	{
		//		if ( jobReqs.length() > 0 )
		//		{
		//			jobReqs.append( " and " );
		//		}
		//		jobReqs.append( "(" + req.toJdf() + ")" );
		//	}
		//}
		//mgJob = new ConnectorJob( jobReqs.toString(), job.getName() );
		//jobReqs = null;

		mgJob = new ConnectorJob(
			( job.getRequirements() != null ) ? job.getRequirements().toString() : null,
			job.getName()
		);

		for ( IBotTask task : job.getTasks() )
		{
			String[] inputs = null;
			StringBuilder comsStr = new StringBuilder();;
			String[] outputs = null;

			// Transform a StageIn object to an array of "stringified" stage-in rules
			if (
				(
					task.getStageIn() != null
					&& !Collections.IsNullOrEmpty( task.getStageIn().getRules() )
				)
				|| (
					job.getStageIn() != null
					&& !Collections.IsNullOrEmpty( job.getStageIn().getRules() )
				)
			)
			{
				List<IStageInRule> rules = null;

				if (
					task.getStageIn() != null
					&& !Collections.IsNullOrEmpty( task.getStageIn().getRules() )
				)
				{
					rules = task.getStageIn().getRules();
				}
				else
				{
					rules = job.getStageIn().getRules();
				}

				inputs = new String[ rules.size() ];

				int i = 0;
				for (IStageInRule rule : rules )
				{
					inputs[ i++ ] = rule.toString();
				}
			}

			if (
				!Collections.IsNullOrEmpty( task.getCommands() )
				|| !Collections.IsNullOrEmpty( job.getCommands() )
			)
			{
				List<IRemoteCommand> comsList;

				if ( !Collections.IsNullOrEmpty( task.getCommands() ) )
				{
					comsList = task.getCommands();
				}
				else
				{
					comsList = job.getCommands();
				}

				for ( IRemoteCommand com : comsList )
				{
					if ( comsStr.length() > 0 )
					{
						comsStr.append( OurGridEnv.GetInstance().getCommandSeparator() );
					}
					comsStr.append( com.toString() );
				}
			}

			// Transform a StageOut object to an array of "stringified" stage-out rules
			if (
				(
					task.getStageOut() != null
					&& !Collections.IsNullOrEmpty( task.getStageOut().getRules() )
				)
				|| (
					job.getStageOut() != null
					&& !Collections.IsNullOrEmpty( job.getStageOut().getRules() )
				)
			)
			{
				List<IStageOutRule> rules = null;

				if (
					task.getStageOut() != null
					&& !Collections.IsNullOrEmpty( task.getStageOut().getRules() )
				)
				{
					rules = task.getStageOut().getRules();
				}
				else
				{
					rules = job.getStageOut().getRules();
				}

				outputs = new String[ rules.size() ];

				int i = 0;
				for (IStageOutRule rule : rules )
				{
					outputs[ i++ ] = rule.toString();
				}
			}

			mgJob.addConnectorTask(
				inputs,
				( comsStr != null ) ? comsStr.toString() : null,
				outputs
			);
		}

		return mgJob;
	}

	@Deprecated
	protected static IBotJob CreateJob(ConnectorJob mgJob)
	{
		//TODO
		return null;
	}
*/
}
