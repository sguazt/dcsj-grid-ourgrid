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

import it.unipmn.di.dcs.common.text.ITextOp;
import it.unipmn.di.dcs.grid.core.middleware.IMiddlewareCapabilities;
import it.unipmn.di.dcs.grid.core.middleware.IMiddlewareManager;
//import it.unipmn.di.dcs.grid.core.middleware.sched.IRequirementOp;
import it.unipmn.di.dcs.grid.core.middleware.sched.IScheduler;
import it.unipmn.di.dcs.grid.core.middleware.sched.Scheduler;
import it.unipmn.di.dcs.grid.core.middleware.WorkerProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Manager class for the OurGrid middleware.
 *
 * @author <a href="mailto:marco.guazzone@gmail.com">Marco Guazzone</a>
 */
public class OurGridMiddlewareManager implements IMiddlewareManager
{
	private IScheduler scheduler; /** The scheduler. */
	private IMiddlewareCapabilities midCaps; /** The middleware capabilities. */
	private WorkerProperties workerProps; /** Attributes usable in job requirements. */
	private List<ITextOp> reqOps; /** Available requirement operators. */

	/** A constructor. */
	public OurGridMiddlewareManager()
	{
		// empty
	}

	/** A constructor. */
	public OurGridMiddlewareManager(OurGridEnv env)
	{
		OurGridEnv.SetInstance(env);
	}

	//@{ IMiddlewareManager implementation

	public IMiddlewareCapabilities getMiddlewareCapabilities()
	{
		if ( this.midCaps == null )
		{
			this.midCaps = new OurGridMiddlewareCapabilities();
		}

		return this.midCaps;
	}

	public IScheduler getJobScheduler()
	{
		if ( this.scheduler == null )
		{
			this.scheduler = new Scheduler(
				new OurGridSchedulerDriver()
			);
		}

		return this.scheduler;
	}

	public WorkerProperties getWorkerProperties()
	{
		// Worker properties correspond in OurGrid to GuMs
		// attributes (or requirements).
		// In OurGrid, requirements are defined inside the peer
		// description file (SDF).
		// So to get them we must communicate with each peer
		// attached to the mygrid scheduler and for each peer
		// collect the attributes for all of its GuMs.
		// The main problem is that MyGrid APIs don't allow to retrieve
		// all the existent GuMs (you can retrieve only the "known"
		// GuMs). So this method must be implemented in another way.

		if ( this.workerProps == null )
		{
			this.workerProps = new WorkerProperties();
			//FIXME: This should be set somewhere (maybe in a file)
			this.workerProps.addToProperty( "os", "linux" ); //FIXME: don't hard-code me!
			this.workerProps.addToProperty( "os", "windows" ); //FIXME: don't hard-code me!
			this.workerProps.addToProperty( "environment", "topix" ); //FIXME: don't hard-code me!
			this.workerProps.addToProperty( "environment", "unipmn" ); //FIXME: don't hard-code me!
		}

		return this.workerProps;
	}

	public List<ITextOp> getJobRequirementOperators()
	{
		return OurGridEnv.GetInstance().getJobRequirementOperators();
	}

/*
	public IJobRequirement createJobRequirement(String s)
	{
		return null;
	}
*/

	//@} IMiddlewareManager implementation
}
