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
import it.unipmn.di.dcs.grid.core.middleware.sched.IJobHandle;
import it.unipmn.di.dcs.grid.core.middleware.sched.ITaskHandle;
import it.unipmn.di.dcs.grid.core.middleware.sched.JobType;

import java.util.ArrayList;
import java.util.List;

import org.ourgrid.mygrid.scheduler.jobmanager.JobEntry;
import org.ourgrid.mygrid.scheduler.jobmanager.TaskEntry;

/**
 * Job Handle for an OurGrid job.
 *
 * @author <a href="mailto:marco.guazzone@gmail.com">Marco Guazzone</a>
 */
public class OurGridJobHandle implements IJobHandle
{
	/** The job identifier. */
	private String id;

	/** The job name. */
	private String name;

//	/** The number of tasks of the job. */
//	private int ntasks;

	/** The tasks of the job. */
	private List<ITaskHandle> tasks = new ArrayList<ITaskHandle>();

	/**
	 * A constructor.
	 */
	public OurGridJobHandle(int jobId)
	{
		this.id = String.valueOf( jobId );

		try
		{
			JobEntry job = null;
			job = OurGridUtil.GetOurGridJob( jobId );
			this.name = job.getLabel();
			//this.ntasks = job.getTasks().size();
			for ( TaskEntry task : job.getTasks() )
			{
				this.tasks.add(
					new OurGridTaskHandle(
						task.getId(),
						this
					)
				);
			}
		}
		catch (OurGridException oge)
		{
			//TODO: propagate exception ...
		}
	}

	/**
	 * Sets the job identifier.
	 */
	public void setId(String value)
	{
		this.id = value;
	}

	@Override
	public String toString()
	{
		return this.id;
	}

	//@{ IJobHandle implementation

	public String getId()
	{
		return this.id;
	}

	public String getName()
	{
		return this.name;
	}

	public JobType getType()
	{
		return JobType.BOT; // In OurGrid, jobs are always BoTs
	}

	public int getTasksNum()
	{
		return this.tasks.size();
	}

	public List<ITaskHandle> getTasks()
	{
		return this.tasks;
	}

	//@} IJobHandle implementation
}
