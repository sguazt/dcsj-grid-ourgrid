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

import it.unipmn.di.dcs.grid.core.middleware.IMiddlewareCapabilities;
import it.unipmn.di.dcs.grid.core.middleware.sched.JobType;
import it.unipmn.di.dcs.grid.core.middleware.sched.StageInMode;
import it.unipmn.di.dcs.grid.core.middleware.sched.StageOutMode;

/**
 * Class for getting capabilities of OurGrid middleware.
 *
 * @author <a href="mailto:marco.guazzone@gmail.com">Marco Guazzone</a>
 */
public class OurGridMiddlewareCapabilities implements IMiddlewareCapabilities
{
	//private static OurGridMiddlewareCapabilities instance; /** The singleton instance */

	/** A constructor */
	//private OurGridMiddlewareCapabilities()
	public OurGridMiddlewareCapabilities()
	{
		// empty
	}

	//public static synchronized OurGridMiddlewareCapabilities GetInstance()
	//{
	//	if ( OurGridMiddlewareCapabilities.instance == null )
	//	{
	//		OurGridMiddlewareCapabilities.instance = new OurGridMiddlewareCapabilities();
	//	}
	//	return OurGridMiddlewareCapabilities.instance;
	//}

	//@{ IMiddlewareCapabilities implementation

	public boolean supportJobType(JobType type)
	{
		if ( type == JobType.BOT || type == JobType.SINGLE )
		{
			return true;
		}

		return false;
	}

	public boolean supportStageInMode(StageInMode mode)
	{
		if ( mode == StageInMode.ALWAYS_OVERWRITE && mode == StageInMode.DIFF_OVERWRITE )
		{
			return true;
		}

		return false;
	}

	public boolean supportStageOutMode(StageOutMode mode)
	{
		if ( mode == StageOutMode.ALWAYS_OVERWRITE )
		{
			return true;
		}

		return false;
	}

	public boolean supportConditionalStageIn()
	{
		// Yes, we can do thing like this:
		// init:
		//       if ( os == linux )
		//       {
		//         put/store statements ...
		//       }
		//       else
		//       {
		//         put/store statements ...
		//       }

		return true;
	}

	public boolean supportConditionalRemoteCommand()
	{
		// No, remote section does not support conditional statements

		return false;
	}

	public boolean supportConditionalStageOut()
	{
		// Yes, we can do thing like this:
		// out:
		//       if ( os == linux )
		//       {
		//         get statements ...
		//       }
		//       else
		//       {
		//         get statements ...
		//       }

		return true;
	}

	//@} IMiddlewareCapabilities implementation
}
