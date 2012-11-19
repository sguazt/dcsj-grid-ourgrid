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

import it.unipmn.di.dcs.grid.core.middleware.sched.ExecutionStatus;
import it.unipmn.di.dcs.grid.core.middleware.sched.SchedulerException;
import it.unipmn.di.dcs.grid.core.middleware.sched.StageInMode;
import it.unipmn.di.dcs.grid.core.middleware.sched.StageOutMode;
import it.unipmn.di.dcs.grid.core.format.jdf.JdfGrammar;

import java.util.Collection;
import java.io.File;

import org.ourgrid.common.config.Configuration;
import org.ourgrid.common.config.LogConfiguration;
import org.ourgrid.mygrid.scheduler.jobmanager.JobEntry;
import org.ourgrid.mygrid.ui.MyGridUIManager;
import org.ourgrid.mygrid.ui.UIManager;

//import org.ourgrid.mygrid.scheduler.ExecutionStatus;

/**
 * Generic utility class for the <em>OurGrid</em> middleware.
 *
 * @author <a href="mailto:marco.guazzone@gmail.com">Marco Guazzone</a>
 */
public final class OurGridUtil
{
	private static boolean MyGridLogPrepared = false;

	public static String StageInModeToString(StageInMode mode)
	{
		switch ( mode )
		{
			case ALWAYS_OVERWRITE:
				return "put";
			case DIFF_OVERWRITE:
				return "store";
		}
		return "unknown";
	}

	public static StageInMode StringToStageInMode(String s)
	{
		if ( s.equals( "store" ) )
		{
			return StageInMode.DIFF_OVERWRITE;
		}
		if ( s.equals( "put" ) )
		{
			return StageInMode.ALWAYS_OVERWRITE;
		}
		return StageInMode.UNKNOWN;
	}

	public static String StageOutModeToString(StageOutMode mode)
	{
		switch ( mode )
		{
			case ALWAYS_OVERWRITE:
				return "get";
		}
		return "unknown";
	}

	public static StageInMode StringToStageOutMode(String s)
	{
		if ( s.equals( "get" ) )
		{
			return StageInMode.ALWAYS_OVERWRITE;
		}
		return StageInMode.UNKNOWN;
	}

	public static ExecutionStatus ToDcsExecStatus(org.ourgrid.mygrid.scheduler.ExecutionStatus status)
	{
		if ( status.isRunning() )
		{
			return ExecutionStatus.RUNNING;
		}
		if ( status.isUnstarted() )
		{
			return ExecutionStatus.UNSTARTED;
		}
		if ( status.isFinished() )
		{
			return ExecutionStatus.FINISHED;
		}
		if ( status.isFailed() )
		{
			return ExecutionStatus.FAILED;
		}
		if ( status.isCancelled() )
		{
			return ExecutionStatus.CANCELLED;
		}
		if ( status.isAborted() )
		{
			return ExecutionStatus.ABORTED;
		}

		return ExecutionStatus.UNKNOWN;
	}

	public static JobEntry GetOurGridJob(int jid) throws OurGridException
	{
		OurGridUtil.PrepareMyGridEnv();

		UIManager services = null;

		try
		{
			services = MyGridUIManager.getInstance();
		}
		catch (java.rmi.RemoteException re)
		{
			throw new OurGridException( "Error while trying to get OurGrid job from job-id.", re );
		}

		return OurGridUtil.GetOurGridJob(
			jid,
			services
		);
	}

	public static JobEntry GetOurGridJob(int jid, UIManager services) throws OurGridException
	{
		Collection<JobEntry> myGridJobs = null;

		try
		{
			myGridJobs = services.getJobs( null );
		}
		catch (java.rmi.RemoteException re)
		{
			throw new OurGridException( "Error while trying to get OurGrid job from job-id.", re );
		}

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
			throw new OurGridException( "There is no job on the scheduler with id '" + jid + "'" );
		}

		return mgjob;
	}

	private static void PrepareMyGridEnv()
	{
		Configuration conf = null;

		// Prepares the configuration singleton instance.
		//
		// Needed because many OurGrid classes assume that a JVM
		// instance contains either MyGrid or Peer or UserAgent related
		// code (i.e., except for the first time, they retrieve the
		// singleton Configuration instance by invoking
		// "Configuration.getInstance()").
		conf = Configuration.getInstance( Configuration.MYGRID );

		// Prepares the logging stuff

		if ( OurGridUtil.MyGridLogPrepared )
		{
			return;
		}

		String logFile = conf.getLogPath();

		String parents = new File( logFile ).getParent();

		if ( parents != null )
		{
			(new File( parents )).mkdirs();
		}

		LogConfiguration.configure( conf.getLogPropertiesPath(), logFile );

		OurGridUtil.MyGridLogPrepared = true;
	}
}
