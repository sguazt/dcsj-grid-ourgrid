/*
 * Copyright (c) 2002-2006 Universidade Federal de Campina Grande
 * 
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 * 
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc., 59 Temple
 * Place, Suite 330, Boston, MA 02111-1307 USA
 */
package it.unipmn.di.dcs.grid.middleware.ourgrid.wrapper;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

import org.ourgrid.common.spec.CompilerMessages;
import org.ourgrid.common.spec.IOBlock;
import org.ourgrid.common.spec.IOEntry;
import org.ourgrid.common.spec.JobSpec;
import org.ourgrid.common.spec.TaskSpec;
import org.ourgrid.common.spec.exception.JobSpecificationException;
import org.ourgrid.common.spec.exception.TaskSpecificationException;
import org.ourgrid.common.spec.main.CommonCompiler;
import org.ourgrid.common.spec.semantic.exception.SemanticException;
import org.ourgrid.common.spec.semantic.SemanticActions;
import org.ourgrid.common.spec.syntactical.CommonSyntacticalAnalyzer;
import org.ourgrid.common.spec.token.Token;

/**
 * This entity is the set of actions that the JOB grammar uses to build a answer
 * to the compilation of sources wrote in this language. Created on 15/06/2004
 */
public class JDFSemanticActionsWrapper implements SemanticActions
{

//	private static transient final org.apache.log4j.Logger LOG = org.apache.log4j.Logger
//			.getLogger( JDFSemanticActions.class );

	private Stack<String> stack;

	private Token actualToken;

	private int mode = CommonSyntacticalAnalyzer.MODE_NORMAL;

	// Needed variables
	private boolean isJobAttrib = true;

	private IOBlock transferEntries = null;

	private LinkedList<TaskSpec> tasksSpec = new LinkedList<TaskSpec>();

	// Job (default) attributes
	private String jobRemoteScript = null;

	private IOBlock jobInputEntries, jobOutputEntries = null;

	// Actual Task attributes
	private String remoteScript = null;

	private IOBlock inputEntries, outputEntries = null;

	private String condition = null;

	private JobSpec theJob = null;

	/* The last action that will be executed has to set it */
	private List<JobSpec> result;


	/**
	 * The constructor
	 */
	public JDFSemanticActionsWrapper() {

		this.stack = new Stack<String>();
	}


	/**
	 * @see org.ourgrid.common.spec.semantic.SemanticActions#performAction(java.lang.String,
	 *      org.ourgrid.common.spec.token.Token)
	 */
	public void performAction( String action, Token token ) throws SemanticException {

		this.actualToken = token;
		try {
			Class semantic = Class.forName( this.getClass().getName() );
			Method method = semantic.getMethod( action );
			method.invoke( this );
		} catch ( NoSuchMethodException nsmex ) {
			throw new SemanticException( CompilerMessages.SEMANTIC_ACTION_NOT_FOUND, nsmex );
		} catch ( ClassNotFoundException cnfex ) {
			throw new SemanticException( CompilerMessages.SEMANTIC_CLASS_NOT_FOUND, cnfex );
		} catch ( InvocationTargetException itex ) {
			if ( itex.getCause() instanceof SemanticException ) {
				throw (SemanticException) itex.getCause();
			}
			throw new SemanticException( CompilerMessages.SEMANTIC_FATAL_ERROR(), itex.getCause() );
		} catch ( IllegalAccessException iaex ) {
			throw new SemanticException( CompilerMessages.SEMANTIC_FATAL_ILLEGAL_ACCESS, iaex );
		}
	}


	/**
	 * @see org.ourgrid.common.spec.semantic.SemanticActions#getOperationalMode()
	 */
	public int getOperationalMode() {

		return this.mode;
	}


	/**
	 * @see org.ourgrid.common.spec.semantic.SemanticActions#getResult()
	 */
	public List<JobSpec> getResult() {

		return this.result;
	}


	/**
	 * This action: Sets the actual script of the remote script. Actual can be
	 * jobs (default) script if this.isJobAttrib == true It happens only when
	 * the first tag "task:" was not found yet.
	 */
	public void action4() {

		if ( this.isJobAttrib == true ) {
			this.jobRemoteScript = actualToken.getSymbol();
		} else {
			this.remoteScript = actualToken.getSymbol();
		}

	}


	/**
	 * This action: Tells to this entity that the job (default) attributes
	 * reading was finished. That means that will begin to read tasks.
	 */
	public void action8() {

		this.isJobAttrib = false;
	}


	/**
	 * This action: Closes a Task and put it at "this.tasksSpec" list.
	 * 
	 * @throws SemanticException When the task could not be validated then the
	 *         TaskSpecificationException is wrapped into this one.
	 */
	public void action9() throws SemanticException {

		checkTaskEntries();
		TaskSpec task;
		try {
			task = new TaskSpec( this.inputEntries, this.remoteScript, this.outputEntries );
			tasksSpec.add( task );
			nullTaskEntries();
		} catch ( TaskSpecificationException tsex ) {
//			LOG.error( "A task could not be validated! - Interrupting the compilation process." );
			throw new SemanticException( CompilerMessages.BAD_TASK_DEFINITION( (tasksSpec.size() + 1), tsex.getCause()
					.getMessage() ) );
		}
	}


	/**
	 * This action: Initializes the "condition" string for the actual block of
	 * I/O commands or job expresion.
	 */
	public void action10() {

		this.condition = new String();
	}


	/**
	 * This action: Concatenates the symbol of the Token at the condition string
	 * statemant.
	 */
	public void action11() {

		this.condition = condition + " " + this.actualToken.getSymbol();
	}


	/**
	 * This action: Sets the condition string to null, it means that the
	 * condition readed will not be used anymore.
	 */
	public void action12() {

		this.condition = null;
	}


	/**
	 * This action: Mounts the condition for the ELSE block and push it at the
	 * stack.
	 */
	public void action13() {

		this.condition = "! ( " + condition.trim() + " )";
		this.stack.push( condition );
	}


	/**
	 * This action: Closes and set the job expresion.
	 */
	public void action14() {

		this.theJob.setRequirements( condition.trim() );
	}


	/**
	 * This action: initializes the IOBlock to receive a new one.
	 */
	public void action15() {

		this.transferEntries = new IOBlock();
	}


	/**
	 * This action: Puts the I/O block condition statement at the stack.
	 */
	public void action16() {

		this.stack.push( this.condition.trim() );
	}


	/**
	 * This action: Will push a empty String object at the stack. It happens
	 * when the I/O entries have no conditions to be transfered.
	 */
	public void action17() {

		this.condition = "";
		this.stack.push( condition );
	}


	/**
	 * This action: Will push the other parts ( command, file, path ) of the I/O
	 * commands at the stack.
	 */
	public void action18() {

		this.stack.push( this.actualToken.getSymbol() );
	}


	/**
	 * This action: Builds a IOEntry and insert it at the actual IOBlock.
	 * 
	 * @throws SemanticException If the user did not define a part of the I/O
	 *         command.
	 */
	public void action19() throws SemanticException {

		String place = stack.pop();
		String filePath = stack.pop();
		String command = stack.pop();
		String condition = stack.peek(); // do not remove it because
		// it can be necessary for
		// other entries.
		IOEntry entry = buildEntry( command, filePath, place );
		this.transferEntries.putEntry( condition, entry );
	}


	/**
	 * This action:Pops the "condition" string that remains at the stack top.
	 */
	public void action20() {

		stack.pop();
	}


	/**
	 * This action: Sets the IOBlock built as the input entry for the
	 * actualTask.
	 */
	public void action21() {

		if ( this.isJobAttrib == true ) {
			this.jobInputEntries = this.transferEntries;
		} else {
			this.inputEntries = this.transferEntries;
		}
	}


	/**
	 * This action: Sets the final result LIST object.
	 * 
	 * @throws SemanticException
	 */
	public void action22() throws SemanticException {

		try {
			this.theJob.setTaskSpecs( this.tasksSpec );
		} catch ( JobSpecificationException e ) {
			throw new SemanticException( "Tried to contruct a Job Spec based on a problematic list of Tasks Specs. " );
		}
		this.result = new LinkedList<JobSpec>();
		result.add( theJob );
	}


	/**
	 * This action: Sets the IOBlock built as the output entry for the
	 * actualTask.
	 */
	public void action23() {

		if ( this.isJobAttrib == true ) {
			this.jobOutputEntries = this.transferEntries;
		} else {
			this.outputEntries = this.transferEntries;
		}
	}


	/**
	 * This action: initializes the obejct JobSpec with the found label.
	 */
	public void action24() {

		theJob = new JobSpec( this.actualToken.getSymbol() );
		mode = CommonSyntacticalAnalyzer.MODE_NORMAL;
	}


	/**
	 * This action: initializes the obeject JobSpec with a empty string because
	 * any label was defined.
	 */
	public void action25() {

		theJob = new JobSpec( "" );
	}


	/**
	 * This action: sets the reading mode to readstring
	 */
	public void action26() {

		mode = CommonSyntacticalAnalyzer.MODE_READSTRING;
	}


	/**
	 * This action: sets the reading mode to normal
	 */
	public void action27() {

		mode = CommonSyntacticalAnalyzer.MODE_NORMAL;
	}


	/**
	 * This action: sets the reading mode to readline
	 */
	public void action28() {

		mode = CommonSyntacticalAnalyzer.MODE_READLINE;
	}


	// /////////// AUXILIAR METHODS /////////////////////

	/*
	 * Make all the entries for a task point to null.
	 */
	private void nullTaskEntries() {

		inputEntries = null;
		remoteScript = null;
		outputEntries = null;
	}


	/*
	 * This method checks if a task has any non defined entry and if exists any
	 * defauld value (from Job) to insert at that.
	 */
	private void checkTaskEntries() {

		if ( inputEntries == null ) {
			if ( jobInputEntries != null )
				inputEntries = jobInputEntries;
			else
				inputEntries = new IOBlock();
		}
		if ( remoteScript == null && jobRemoteScript != null ) {
			remoteScript = jobRemoteScript;
		}

		if ( outputEntries == null ) {
			if ( jobOutputEntries != null )
				outputEntries = jobOutputEntries;
			else
				outputEntries = new IOBlock();
		}
	}


	/*
	 * Builds a IOEntry object making the local file paths absolute ones. @param
	 * command the command of the I/O operation, that can be - GET, PUT or STORE
	 * @param filePath the file path of the source @param place the file path of
	 * the destiny @return a IOEntry object referente of the attributes but,
	 * including a absolute path for the local ( that will be found at the home
	 * machine ) file paths using the description file parent. @throws
	 * SemanticException if any of the paramethers is a empty string
	 */
	private IOEntry buildEntry( String command, String filePath, String place ) throws SemanticException {

		if ( command.equals( "" ) || filePath.equals( "" ) || place.equals( "" ) ) {
			throw new SemanticException( CompilerMessages.SEMANTIC_MALFORMED_IO_COMMAND );
		}
		String localParentDir = CommonCompilerWrapper.getSourceParentDir();
		if ( command.equalsIgnoreCase( "GET" ) ) {
			// To insert the JDF parent directory in relative path
			File temp = new File( place );
			if ( !temp.isAbsolute() )
				place = localParentDir + File.separator + place;

		} else { // command is PUT or STORE
			// To insert the JDF parent directory in relative path
			File temp = new File( filePath );
			if ( !temp.isAbsolute() )
				filePath = localParentDir + File.separator + filePath;
		}

		return new IOEntry( command, filePath, place );
	}

}
