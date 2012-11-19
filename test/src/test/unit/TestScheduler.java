/*
 * Copyright (C) 2008  Distributed Computing System (DCS) Group, Computer
 * Science Department - University of Piemonte Orientale, Alessandria (Italy).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package test.unit;

import it.unipmn.di.dcs.common.text.*;

import it.unipmn.di.dcs.grid.core.middleware.IMiddlewareManager;
import it.unipmn.di.dcs.grid.core.middleware.sched.*;

import it.unipmn.di.dcs.grid.middleware.ourgrid.OurGridMiddlewareManager;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.*;
import static org.junit.Assert.*;
import org.junit.runner.JUnitCore;

/**
 * @author <a href="mailto:marco.guazzone@gmail.com">Marco Guazzone</a>
 */
public class TestScheduler
{
	private static final Logger Log = Logger.getLogger(TestScheduler.class.getName());

	private static IJob CreateTestSingleJob()
	{
		SingleJob job = new SingleJob("single-job");
		job.setRequirements(
			new JobRequirements(
				new BinaryTextExpr(
					new TerminalTextExpr("blender"),
					new BinaryTextOp("==","Equality"),
					new TerminalTextExpr("true")
				)
			)
		);
		job.setStageIn(
			new StageIn(
				new StageInRule(
					new StageInAction(
						StageInMode.ALWAYS_OVERWRITE,
						"in-local",
						"in-remote",
						StageInType.VOLATILE
					)
				)
			)
		);
		job.addCommand(
			new RemoteCommand("blender in-remote")
		);
		job.setStageOut(
			new StageOut(
				new StageOutRule(
					new StageOutAction(
						StageOutMode.ALWAYS_OVERWRITE,
						"out-remote",
						"out-local"
					)
				)
			)
		);

		return job;
	}

	private static IBotJob CreateTestBotJob()
	{
		BotJob job = new BotJob("bot-job");

		job.setRequirements(
			new JobRequirements(
				new BinaryTextExpr(
					new TerminalTextExpr("blender"),
					new BinaryTextOp("==","Equality"),
					new TerminalTextExpr("true")
				)
			)
		);

		for (int i = 1; i <= 3; i++)
		{
			BotTask task = new BotTask();

			task.setStageIn(
				new StageIn(
					new StageInRule(
						new StageInAction(
							StageInMode.ALWAYS_OVERWRITE,
							"in-local-" + i,
							"in-remote-" + i,
							StageInType.VOLATILE
						)
					)
				)
			);
			task.addCommand(
				new RemoteCommand("blender in-remote")
			);
			task.setStageOut(
				new StageOut(
					new StageOutRule(
						new StageOutAction(
							StageOutMode.ALWAYS_OVERWRITE,
							"out-remote-" + i,
							"out-local-" + i + ".$JOB.$TASK"
						)
					)
				)
			);
			job.addTask( task );
		}

		return job;
	}

	@Before
	public void setUp()
	{
		Log.info("Setting-up test suite...");

		Log.info("Set-up test suite");
	}

	@Test
	public void testAbortJob()
	{
		Log.info("Entering the 'Abort Job' test...");

		boolean allOk = false;

		try
		{
			Log.info("Retrieving the middleware manager object...");

			IMiddlewareManager midMngr = null;
			midMngr = new OurGridMiddlewareManager();

			Log.info("Retrieving the scheduler object...");

			IScheduler sched = null;
			sched = midMngr.getJobScheduler();

			Log.info("Submitting a test job to the scheduler...");

			IJobHandle jhnd  = null;
			jhnd = sched.submitJob( CreateTestBotJob() );

			Log.info("Aborting the submitted job...");

			sched.abortJob( jhnd );

			allOk = true;
		}
		catch (Exception e)
		{
			Log.log(Level.SEVERE, "Caught exception.", e);
			allOk = false;
		}

		Log.info("Exiting the 'Abort Job' test...");

		assertTrue( allOk );
	}

	@After
	public void tearDown()
	{
	}

	public static void main(String[] args)
	{
		 org.junit.runner.JUnitCore.main(
			TestScheduler.class.getName()
		);
	}
}
