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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;

import org.ourgrid.common.spec.main.CommonCompiler;
import org.ourgrid.common.spec.main.CompilerException;

import org.ourgrid.common.spec.CommonCModulesFactory;
import org.ourgrid.common.spec.CompilerMessages;
import org.ourgrid.common.spec.CompilerModulesFactory;
import org.ourgrid.common.spec.GumSpec;
import org.ourgrid.common.spec.grammar.CommonGrammar;
import org.ourgrid.common.spec.grammar.io.GalsGrammarReader;
import org.ourgrid.common.spec.grammar.io.MalformedGrammarException;
import org.ourgrid.common.spec.lexical.CommonLexicalAnalyzer;
import org.ourgrid.common.spec.semantic.CommonSemanticAnalyzer;
import org.ourgrid.common.spec.semantic.GDFSemanticActions;
import org.ourgrid.common.spec.semantic.JDFSemanticActions;
import org.ourgrid.common.spec.semantic.SDFSemanticActions;
import org.ourgrid.common.spec.semantic.SemanticActions;
import org.ourgrid.common.spec.semantic.SemanticAnalyzer;
import org.ourgrid.common.spec.syntactical.CommonSyntacticalAnalyzer;
import org.ourgrid.common.spec.syntactical.SyntacticalAnalyzer;
import org.ourgrid.common.spec.syntactical.SyntacticalException;

/**
 * Wrapper for common description file compiler.
 *
 * @author <a href="mailto:marco.guazzone@gmail.com">Marco Guazzone</a>
 */
public class CommonCompilerWrapper extends CommonCompiler
{

	public static final String JDF_TYPE = "JOB";

	public static final String GDF_TYPE = "GDF";

	public static final String SDF_TYPE = "SDF";

	private String GDF_FILE_NAME = "resources/specs/GDFGrammar.gals";

	private String SDF_FILE_NAME = "resources/specs/SDFGrammar.gals";

	private String JDF_FILE_NAME = "resources/specs/JDFGrammar.gals";

	private static File sourceFile;

	private InputStream grammarFileStream;

//	private static transient final org.apache.log4j.Logger LOG = org.apache.log4j.Logger
//			.getLogger( CommonCompiler.class );

	private CompilerModulesFactory factory;

	private SemanticActions actions;

	/* The last action that will be executed has to set it */
	//private List<GumSpec> result;
	private List result;

        /**
	 * Initialize the object.
	 */
        public CommonCompilerWrapper()
	{
                this.factory = new CommonCModulesFactory();
        }

        /**
	 * @see org.ourgrid.common.spec.main.Compiler#compile(String, String)
	 */
	public void compile( String sourceFileName, String languageType ) throws CompilerException
	{
		this.openFiles( sourceFileName, languageType );
		CommonSemanticAnalyzer semanticAnalyzer = this.buildSemanticAnalyzer( languageType );
		CommonSyntacticalAnalyzer syntactical = (CommonSyntacticalAnalyzer) this.buildSyntacticalAnalyzer( semanticAnalyzer );
		this.run( syntactical );
		this.result = this.actions.getResult();
	}

        /**
	 * Starts the compilation at the given syntactical analyzer.
	 * 
	 * @param syntactical A syntactical analyzer.
	 * @throws CompilerException
	 */
	private void run( CommonSyntacticalAnalyzer syntactical ) throws CompilerException
	{
		try
		{
			syntactical.startCompilation();
		}
		catch ( SyntacticalException sex )
		{
			throw new CompilerException( sex.getMessage(), sex );
		}
	}

	/**
	 * Builds all the entities necessaries to build a syntactiacal analyzer and
	 * return it.
	 * 
	 * @param semantic The SemanticAnalyzer
	 * @return A SyntacticalAnalyzer
	 * @throws CompilerException
	 */
	private SyntacticalAnalyzer buildSyntacticalAnalyzer( SemanticAnalyzer semantic ) throws CompilerException
	{
		GalsGrammarReader reader = new GalsGrammarReader();
		CommonGrammar grammar = new CommonGrammar();
		this.buildGrammar( reader, grammar );
		CommonLexicalAnalyzer lexical = (CommonLexicalAnalyzer) this.factory.createLexicalAnalyzer( this.sourceFile.getAbsolutePath() );
		CommonSyntacticalAnalyzer syntactical;
		syntactical = (CommonSyntacticalAnalyzer) this.factory.createSyntacticalAnalyzer( lexical, grammar, semantic );

		return syntactical;
	}

	/*
	 * Will read the right grammar file, create the grammar object and prepare
	 * it for use. @param reader The specific grammar reader. @param grammar The
	 * grammar object that will be filled up with the informations read from
	 * grammar file source.
	 */
	private void buildGrammar( GalsGrammarReader reader, CommonGrammar grammar ) throws CompilerException
	{
		try
		{
			reader.read( grammarFileStream, grammar );
			grammar.loadSyntacticTable();
		}
		catch (MalformedGrammarException mfex)
		{
			throw new CompilerException( CompilerMessages.BAD_GRAMMAR_FILE_STRUCTURE, mfex );
		}
		catch (FileNotFoundException fnfex)
		{
			throw new CompilerException( CompilerMessages.BAD_GRAMMAR_FILE_NOT_FOUND, fnfex );
		}
		catch (IOException ioex)
		{
			throw new CompilerException( CompilerMessages.ERROR_GRAMMAR_IO, ioex );
		}
	}

	/**
	 * Will open and check the "good health" of the specified objects: the
	 * source file and the grammar file related to languageType.
	 * 
	 * @param source the source file to be compiled.
	 * @param languageType the language type of the file to be compiled.
	 * @throws CompilerException If any of the files could not be found or read.
	 */
	private void openFiles( String source, String languageType ) throws CompilerException
	{
		// Validating the files
		CommonCompilerWrapper.sourceFile = new File( source );
		if ( (!sourceFile.exists()) || (!sourceFile.canRead()) )
		{
			IOException ioex = new IOException( CompilerMessages.BAD_SOURCE_FILE( sourceFile.getAbsolutePath() ) );
			throw new CompilerException( ioex.getMessage(), ioex );
		}

		URL resourceURL = null;
		if ( languageType.equalsIgnoreCase( JDF_TYPE ) )
		{
			resourceURL = Thread.currentThread().getContextClassLoader().getResource( JDF_FILE_NAME );
		}
		else if ( languageType.equalsIgnoreCase( GDF_TYPE ) )
		{
			resourceURL = Thread.currentThread().getContextClassLoader().getResource( GDF_FILE_NAME );
		}
		else if ( languageType.equalsIgnoreCase( SDF_TYPE ) )
		{
			resourceURL = Thread.currentThread().getContextClassLoader().getResource( SDF_FILE_NAME );
		}
		else
		{
			throw new CompilerException( CompilerMessages.BAD_LANGUAGE_TYPE );
		}

		try
		{
			this.grammarFileStream = resourceURL.openStream();
		}
		catch ( IOException e )
		{
			throw new CompilerException( CompilerMessages.BAD_GRAMMAR_FILE_IOPROBLEMS, e );
		}
	}

        /**
	 * Will build a semantic analyzer to the specific given language type.
	 * 
	 * @param languageType the language type of the source that will be
	 *        compiled.
	 * @return the SemanticAnalyzer for the language type.
	 * @throws CompilerException If the semantic creation fails.
	 */
	private CommonSemanticAnalyzer buildSemanticAnalyzer( String languageType ) throws CompilerException
	{
		CommonSemanticAnalyzer analyzer;
		if ( languageType.equalsIgnoreCase( JDF_TYPE ) )
		{
			this.actions = new JDFSemanticActionsWrapper();
			analyzer = (CommonSemanticAnalyzer) this.factory.createSemanticAnalyzer( actions );
		}
		else if ( languageType.equalsIgnoreCase( GDF_TYPE ) )
		{
			this.actions = new GDFSemanticActions();
			analyzer = (CommonSemanticAnalyzer) this.factory.createSemanticAnalyzer( actions );
		}
		else if ( languageType.equalsIgnoreCase( SDF_TYPE ) )
		{
			this.actions = new SDFSemanticActions();
			analyzer = (CommonSemanticAnalyzer) this.factory.createSemanticAnalyzer( actions );
		}
		else
		{
			throw new CompilerException( CompilerMessages.BAD_LANGUAGE_TYPE );
		}
		return analyzer;
	}

        /**
	 * @see org.ourgrid.common.spec.main.Compiler#getResult()
	 */
	public List getResult()
	{
		return this.result;
	}

        /**
	 * @return Returns the parent directory of the source that is been compiled.
	 */
	public static String getSourceParentDir()
	{
		return CommonCompilerWrapper.sourceFile.getAbsoluteFile().getParent();
	}
}
