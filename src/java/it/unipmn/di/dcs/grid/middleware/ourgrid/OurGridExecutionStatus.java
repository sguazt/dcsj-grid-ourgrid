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

import it.unipmn.di.dcs.grid.core.middleware.sched.IExecutionStatus;
import org.ourgrid.mygrid.scheduler.ExecutionStatus;

/**
 * Executions status class for OurGrid middleware.
 *
 * @author <a href="mailto:marco.guazzone@gmail.com">Marco Guazzone</a>
 */
public class OurGridExecutionStatus implements IExecutionStatus
{
	private ExecutionStatus status;

	public OurGridExecutionStatus(ExecutionStatus status)
	{
		this.status = status;
	}

	@Override
	public String toString()
	{
		return this.status.toString();
	}

	//@{ IExecutionStatus implementation

	public boolean isRunning()
	{
		return this.status.isRunning();
	}

	public boolean isUnstarted()
	{
		return this.status.isUnstarted();
	}

	public boolean isFinished()
	{
		return this.status.isFinished();
	}

	public boolean isFailed()
	{
		return this.status.isFailed();
	}

	public boolean isCancelled()
	{
		return this.status.isCancelled();
	}

	public boolean isAborted()
	{
		return this.status.isAborted();
	}

	//@} IExecutionStatus implementation
} 
