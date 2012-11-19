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

import it.unipmn.di.dcs.grid.core.middleware.sched.IJobHandle;
import it.unipmn.di.dcs.grid.core.middleware.sched.ITaskHandle;

/**
 * Task Handle for an OurGrid job.
 *
 * @author <a href="mailto:marco.guazzone@gmail.com">Marco Guazzone</a>
 */
public class OurGridTaskHandle implements ITaskHandle
{
	/** The job identifier. */
	private String id;

	/** The handle of the parent job. */
	private IJobHandle jobHnd;

	/**
	 * A constructor.
	 */
	public OurGridTaskHandle(int taskId, IJobHandle jobHnd)
	{
		this.id = String.valueOf( taskId );
		this.jobHnd = jobHnd;
	}

	public void setId(String value)
	{
		this.id = value;
	}

	public void setJobHandle(IJobHandle value)
	{
		this.jobHnd = value;
	}

	@Override
	public String toString()
	{
		return this.id;
	}

	//@{ ITaskHandle implementation

	public String getId()
	{
		return this.id;
	}

	public IJobHandle getJobHandle()
	{
		return this.jobHnd;
	}

	//@} ITaskHandle implementation
}
