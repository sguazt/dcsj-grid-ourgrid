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

//import it.unipmn.di.dcs.grid.core.middleware.sched.IRequirementOp;
//import it.unipmn.di.dcs.grid.core.middleware.sched.RequirementOpType;
import it.unipmn.di.dcs.grid.core.format.jdf.JdfExporter;
import it.unipmn.di.dcs.grid.core.middleware.sched.JobRequirementsOps;
import it.unipmn.di.dcs.common.text.BinaryTextOp;
import it.unipmn.di.dcs.common.text.ITextOp;
import it.unipmn.di.dcs.common.text.TextOpType;
import it.unipmn.di.dcs.common.text.UnaryTextOp;
import it.unipmn.di.dcs.common.util.Strings;

import java.util.ArrayList;
import java.util.List;

import org.ourgrid.common.config.Configuration;

/**
 * Holds information about OurGrid environment.
 *
 * @author <a href="mailto:marco.guazzone@gmail.com">Marco Guazzone</a>
 */
public class OurGridEnv
{
	private static OurGridEnv instance; /** The singleton instance. */
;
	private static List<ITextOp> SupportedJobRequirementOps; // supported operators respect to the general middleware layer

	private boolean initialized = false;

	static
	{
		OurGridEnv.SupportedJobRequirementOps = new ArrayList<ITextOp>();
		OurGridEnv.SupportedJobRequirementOps.add( JobRequirementsOps.EQ );
		OurGridEnv.SupportedJobRequirementOps.add( JobRequirementsOps.GE );
		OurGridEnv.SupportedJobRequirementOps.add( JobRequirementsOps.GT );
		OurGridEnv.SupportedJobRequirementOps.add( JobRequirementsOps.LE );
		OurGridEnv.SupportedJobRequirementOps.add( JobRequirementsOps.LOGICAL_AND );
		OurGridEnv.SupportedJobRequirementOps.add( JobRequirementsOps.LOGICAL_NOT );
		OurGridEnv.SupportedJobRequirementOps.add( JobRequirementsOps.LOGICAL_OR );
		OurGridEnv.SupportedJobRequirementOps.add( JobRequirementsOps.LT );
		OurGridEnv.SupportedJobRequirementOps.add( JobRequirementsOps.NE );

		if ( Strings.IsNullOrEmpty( System.getProperty("MGROOT") ) )
		{
			// Guess a value for MGROOT
			if ( !Strings.IsNullOrEmpty( System.getenv( "MGROOT" ) ) )
			{
				System.getProperties().setProperty( "MGROOT", System.getenv( "MGROOT" ) );
			}
			else
			{
				System.getProperties().setProperty( "MGROOT", "." );
			}
		}
	}

	protected OurGridEnv()
	{
		//empty
	}

	public static synchronized OurGridEnv GetInstance()
	{
		if ( OurGridEnv.instance == null )
		{
			OurGridEnv.instance = new OurGridEnv();
		}

		return OurGridEnv.instance;
	}

	public static synchronized void SetInstance(OurGridEnv env)
	{
		OurGridEnv.instance = env;
	}

	public synchronized void initialize()
	{
		if ( this.initialized )
		{
			return;
		}

		if ( Strings.IsNullOrEmpty( System.getProperty("MGROOT") ) )
		{
			// Guess a value for MGROOT
			if ( !Strings.IsNullOrEmpty( System.getenv( "MGROOT" ) ) )
			{
				System.getProperties().setProperty( "MGROOT", System.getenv( "MGROOT" ) );
			}
			else
			{
				System.getProperties().setProperty( "MGROOT", "." );
			}
		}

		this.initialized = true;
	}

//	public synchronized void setSchedulerName(String value)
//	{
//		Configuration.getInstance( Configuration.MYGRID ).setProperty( Configuration.PROP_NAME, value );
//	}
//
//	public synchronized String getSchedulerName()
//	{
//		return Configuration.getInstance( Configuration.MYGRID ).getProperty( Configuration.PROP_NAME );
//	}
//
//	public synchronized void setSchedulerPort(int value)
//	{
//		Configuration.getInstance( Configuration.MYGRID ).setProperty( Configuration.PROP_PORT, Integer.toString( value ) );
//	}
//
//	public synchronized int getSchedulerPort()
//	{
//		return Integer.parseInt(
//			Configuration.getInstance( Configuration.MYGRID ).getProperty( Configuration.PROP_PORT )
//		);
//	}

	public String getCommandSeparator()
	{
		return JdfExporter.CommandSeparator();
	}

	public String getJobIdVar()
	{
		return JdfExporter.JobIdVar();
	}

	public String getJobTaskIdVar()
	{
		return JdfExporter.JobTaskIdVar();
	}

	public String getRemotePersistentPathVar()
	{
		return JdfExporter.RemotePersistentPathVar();
	}

	public String getRemoteProcessorVar()
	{
		return JdfExporter.RemoteProcessorVar();
	}

	public String getRemoteVolatilePathVar()
	{
		return JdfExporter.RemoteVolatilePathVar();
	}

	public List<ITextOp> getJobRequirementOperators()
	{
		return OurGridEnv.SupportedJobRequirementOps;
	}
}
