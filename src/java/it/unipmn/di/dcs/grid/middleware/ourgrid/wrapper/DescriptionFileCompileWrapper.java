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

package it.unipmn.di.dcs.grid.middleware.ourgrid.wrapper;

import java.util.List;

import org.ourgrid.common.spec.GumSpec;
import org.ourgrid.common.spec.JobSpec;
import org.ourgrid.common.spec.main.CommonCompiler;
import org.ourgrid.common.spec.main.CompilerException;
import org.ourgrid.common.spec.main.DescriptionFileCompile;
import org.ourgrid.common.spec.PeerSpec;
import org.ourgrid.mygrid.ui.command.UIMessages;

/**
 * Wrapper for OurGrid description file compiler.
 *
 * @author <a href="mailto:marco.guazzone@gmail.com">Marco Guazzone</a>
 */
public class DescriptionFileCompileWrapper extends DescriptionFileCompile
{
	@SuppressWarnings("unchecked")
	public static JobSpec compileJDF(String descriptionFilePath) throws CompilerException
	{
		CommonCompilerWrapper compiler = new CommonCompilerWrapper();

		compiler.compile( descriptionFilePath, CommonCompilerWrapper.JDF_TYPE );

		List<JobSpec> answer = (List<JobSpec>) compiler.getResult();

		if ( answer == null )
		{
			throw new CompilerException( UIMessages.THE_DESCRIPTION_FILE_IS_EMPTY );
		}

		return answer.get( 0 );
	}

	@SuppressWarnings("unchecked")
	public static List<PeerSpec> compileGDF( String descriptionFilePath ) throws CompilerException {

		CommonCompilerWrapper compiler = new CommonCompilerWrapper();

		try {

			compiler.compile( descriptionFilePath, CommonCompilerWrapper.GDF_TYPE );
		} catch ( CompilerException cex ) {
			throw cex;

		}

		List<PeerSpec> answer = (List<PeerSpec>) compiler.getResult();

		if ( answer == null ) {
			throw new CompilerException( UIMessages.THE_DESCRIPTION_FILE_IS_EMPTY );
		}

		return answer;
	}

	@SuppressWarnings("unchecked")
	public static List<GumSpec> compileSDF( String descriptionFilePath ) throws CompilerException {

		CommonCompilerWrapper compiler = new CommonCompilerWrapper();

		compiler.compile( descriptionFilePath, CommonCompilerWrapper.SDF_TYPE );

		List<GumSpec> answer = (List<GumSpec>) compiler.getResult();

		if ( answer == null ) {
			throw new CompilerException( UIMessages.THE_DESCRIPTION_FILE_IS_EMPTY );
		}

		return answer;
	}
}
