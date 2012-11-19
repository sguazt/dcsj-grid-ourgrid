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

import org.ourgrid.common.config.Configuration;

/**
 * Holds information about OurGrid Scheduler environment.
 *
 * @author <a href="mailto:marco.guazzone@gmail.com">Marco Guazzone</a>
 */
public class OurGridSchedulerEnv extends OurGridEnv
{
	private static OurGridSchedulerEnv instance; /** The singleton instance. */
;
	private Configuration conf = null;

	private boolean initialized = false;

	protected OurGridSchedulerEnv()
	{
		super();
	}

	public static synchronized OurGridSchedulerEnv GetInstance()
	{
		if ( OurGridSchedulerEnv.instance == null )
		{
			OurGridSchedulerEnv.instance = new OurGridSchedulerEnv();
		}

		return OurGridSchedulerEnv.instance;
	}

	public static synchronized void SetInstance(OurGridSchedulerEnv env)
	{
		OurGridSchedulerEnv.instance = env;
	}

	public synchronized void initialize()
	{
		if ( this.initialized )
		{
			return;
		}

		super.initialize();

		this.conf = Configuration.getInstance( Configuration.MYGRID );

		this.initialized = true;
	}

	public synchronized void setSchedulerName(String value)
	{
		this.conf.setProperty( Configuration.PROP_NAME, value );
	}

	public synchronized String getSchedulerName()
	{
		return this.conf.getProperty( Configuration.PROP_NAME );
	}

	public synchronized void setSchedulerPort(int value)
	{
		this.conf.setProperty( Configuration.PROP_PORT, Integer.toString( value ) );
	}

	public synchronized int getSchedulerPort()
	{
		return Integer.parseInt(
			this.conf.getProperty( Configuration.PROP_PORT )
		);
	}
}
