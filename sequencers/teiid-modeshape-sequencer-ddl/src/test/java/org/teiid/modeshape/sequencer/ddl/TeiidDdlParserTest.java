/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.modeshape.sequencer.ddl;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.teiid.modeshape.sequencer.ddl.node.AstNode;

/**
 * A test class for the {@link TeiidDdlParser}.
 */
public class TeiidDdlParserTest extends DdlParserTestHelper implements TeiidDdlConstants {

    public static final String DDL_FILE_PATH = "ddl/";

    @Before
    public void beforeEach() {
        this.parser = new TeiidDdlParser();
        setRootNode(this.parser.nodeFactory().node("DdlRootNode"));
        this.scorer = new DdlParserScorer();
    }

    /**
     * See Teiid TestDDLParser#testDuplicateFunctions()
     */
    @Test
    public void shouldParseDuplicateFunctions() {
        final String functionName = "SourceFunc";
        final String content = "CREATE FUNCTION " + functionName + "() RETURNS string; CREATE FUNCTION " + functionName
                               + "(param string) RETURNS string";
        assertScoreAndParse(content, null, 2);

        final List<AstNode> kids = getRootNode().getChildren();
        assertMixinType(kids.get(0), TeiidDdlLexicon.CreateProcedure.FUNCTION_STATEMENT);
        assertMixinType(kids.get(1), TeiidDdlLexicon.CreateProcedure.FUNCTION_STATEMENT);
    }

    /**
     * See Teiid TestDDLParser#testDuplicateFunctions1()
     */
    @Test
    public void shouldParseDuplicateFunctions1() {
        final String content = "CREATE FUNCTION SourceFunc() RETURNS string OPTIONS (UUID 'a'); CREATE FUNCTION SourceFunc1() RETURNS string OPTIONS (UUID 'a')";
        assertScoreAndParse(content, null, 2);

        {
            final List<AstNode> kids = getRootNode().childrenWithName("SourceFunc");
            assertThat(kids.size(), is(1));
            assertMixinType(kids.get(0), TeiidDdlLexicon.CreateProcedure.FUNCTION_STATEMENT);
        }

        {
            final List<AstNode> kids = getRootNode().childrenWithName("SourceFunc1");
            assertThat(kids.size(), is(1));
            assertMixinType(kids.get(0), TeiidDdlLexicon.CreateProcedure.FUNCTION_STATEMENT);
        }
    }

    /**
     * See Teiid TestDDLParser#testMultipleCommands()
     */
    @Test
    public void shouldParseMultipleCommands() {
        final String content = "CREATE VIEW V1 AS SELECT * FROM PM1.G1 "
                               + "CREATE PROCEDURE FOO(P1 integer) RETURNS (e1 integer, e2 varchar) AS SELECT * FROM PM1.G1;";
        assertScoreAndParse(content, null, 2);

        {
            final List<AstNode> kids = getRootNode().childrenWithName("V1");
            assertThat(kids.size(), is(1));
            assertMixinType(kids.get(0), TeiidDdlLexicon.CreateTable.VIEW_STATEMENT);
        }

        {
            final List<AstNode> kids = getRootNode().childrenWithName("FOO");
            assertThat(kids.size(), is(1));
            assertMixinType(kids.get(0), TeiidDdlLexicon.CreateProcedure.PROCEDURE_STATEMENT);
        }
    }
    
    @Test
    public void shouldKeepCacheHints() {
	// @formatter :off
	final String content = "CREATE VIEW internal_short_ttl (\n" 
	        + "\tcustomer_id integer NOT NULL,\n"
		+ "\ttotal_amount integer\n" 
	        + ") OPTIONS (MATERIALIZED 'TRUE',\n"
		+ "\t\"teiid_rel:MATVIEW_BEFORE_LOAD_SCRIPT\" 'execute Source.native(''INSERT INTO check_table(id,before_load) VALUES (''internal_short_ttl'',1) ON DUPLICATE KEY UPDATE before_load=before_load+1;'');',\n"
		+ "\t\"teiid_rel:MATVIEW_AFTER_LOAD_SCRIPT\" 'execute Source.native(''INSERT INTO check_table(id,after_load) VALUES (''internal_short_ttl'',1) ON DUPLICATE KEY UPDATE after_load=after_load+1;'')'\n"
		+ ")\n"
		+ "\tAS /*+ cache(ttl:100)*/SELECT c.id AS customer_id, CONVERT(SUM(o.amount),biginteger) AS total_amount FROM customers c INNER JOIN orders o ON c.id = o.customer_id GROUP BY c.id;";
	// @formatter :on
	assertScoreAndParse(content, null, 1);

	final List<AstNode> kids = getRootNode().childrenWithName("internal_short_ttl");
	assertThat(kids.size(), is(1));
	assertMixinType(kids.get(0), TeiidDdlLexicon.CreateTable.VIEW_STATEMENT);

	final AstNode viewNode = kids.get(0);
	assertProperty(viewNode, TeiidDdlLexicon.CreateTable.QUERY_EXPRESSION,
		"/*+ cache(ttl:100)*/SELECT c.id AS customer_id, CONVERT(SUM(o.amount),biginteger) AS total_amount FROM customers c INNER JOIN orders o ON c.id = o.customer_id GROUP BY c.id");
    }

    @Test
    public void shouldKeepDdlComments() {
	final String content = "CREATE VIEW PRODUCTDATA (\n"
		+ "\tINSTR_ID string(10) NOT NULL OPTIONS(NAMEINSOURCE '\"INSTR_ID\"', NATIVE_TYPE 'VARCHAR2', UPDATABLE 'FALSE'),\n"
		+ "\tNAME string(60) OPTIONS(NAMEINSOURCE '\"NAME\"', NATIVE_TYPE 'VARCHAR2', UPDATABLE 'FALSE'),\n"
		+ "\tTYPE string(15) OPTIONS(NAMEINSOURCE '\"TYPE\"', NATIVE_TYPE 'VARCHAR2', UPDATABLE 'FALSE'),\n"
		+ "\tISSUER string(10) OPTIONS(NAMEINSOURCE '\"ISSUER\"', NATIVE_TYPE 'VARCHAR2', UPDATABLE 'FALSE'),\n"
		+ "\tEXCHANGE string(10) OPTIONS(NAMEINSOURCE '\"EXCHANGE\"', NATIVE_TYPE 'VARCHAR2', UPDATABLE 'FALSE'),\n"
		+ "\tISDJI bigdecimal(22) NOT NULL OPTIONS(NAMEINSOURCE '\"ISDJI\"', NATIVE_TYPE 'NUMBER', CASE_SENSITIVE 'FALSE', UPDATABLE 'FALSE', FIXED_LENGTH 'TRUE', SEARCHABLE 'ALL_EXCEPT_LIKE'),\n"
		+ "\tISSP500 bigdecimal(22) NOT NULL OPTIONS(NAMEINSOURCE '\"ISSP500\"', NATIVE_TYPE 'NUMBER', CASE_SENSITIVE 'FALSE', UPDATABLE 'FALSE', FIXED_LENGTH 'TRUE', SEARCHABLE 'ALL_EXCEPT_LIKE'),\n"
		+ "\tISNAS100 bigdecimal(22) NOT NULL OPTIONS(NAMEINSOURCE '\"ISNAS100\"', NATIVE_TYPE 'NUMBER', CASE_SENSITIVE 'FALSE', UPDATABLE 'FALSE', FIXED_LENGTH 'TRUE', SEARCHABLE 'ALL_EXCEPT_LIKE'),\n"
		+ "\tISAMEXINT bigdecimal(22) NOT NULL OPTIONS(NAMEINSOURCE '\"ISAMEXINT\"', NATIVE_TYPE 'NUMBER', CASE_SENSITIVE 'FALSE', UPDATABLE 'FALSE', FIXED_LENGTH 'TRUE', SEARCHABLE 'ALL_EXCEPT_LIKE'),\n"
		+ "\tPRIBUSINESS string(30) OPTIONS(NAMEINSOURCE '\"PRIBUSINESS\"', NATIVE_TYPE 'VARCHAR2', UPDATABLE 'FALSE'),\n"
		+ "\tCONSTRAINT PK_PD_INSTR_ID PRIMARY KEY(INSTR_ID)\n"
		+ ") OPTIONS(NAMEINSOURCE '\"PRODUCTS\".\"PRODUCTDATA\"', UPDATABLE 'FALSE') \n" + "AS\n"
		+ "\t/* first comment in the product data*/\n" + "SELECT\n" + "\t\t*\n" + "\tFROM\n"
		+ "\t\t/* second comment in the product data*/\n" + "\t\tProducts.PRODUCTDATA;\n" + "\t\t\n"
		+ "CREATE VIEW PRODUCTSYMBOLS (\n"
		+ "\tINSTR_ID string(10) NOT NULL OPTIONS(NAMEINSOURCE '\"INSTR_ID\"', NATIVE_TYPE 'VARCHAR2', UPDATABLE 'FALSE'),\n"
		+ "\tSYMBOL_TYPE bigdecimal(22) OPTIONS(NAMEINSOURCE '\"SYMBOL_TYPE\"', NATIVE_TYPE 'NUMBER', CASE_SENSITIVE 'FALSE', UPDATABLE 'FALSE', FIXED_LENGTH 'TRUE', SEARCHABLE 'ALL_EXCEPT_LIKE'),\n"
		+ "\tSYMBOL string(10) NOT NULL OPTIONS(NAMEINSOURCE '\"SYMBOL\"', NATIVE_TYPE 'VARCHAR2', UPDATABLE 'FALSE'),\n"
		+ "\tCUSIP string(10) OPTIONS(NAMEINSOURCE '\"CUSIP\"', NATIVE_TYPE 'VARCHAR2', UPDATABLE 'FALSE'),\n"
		+ "\tCONSTRAINT PK_PS_INSTR_ID PRIMARY KEY(INSTR_ID),\n"
		+ "\tCONSTRAINT FK_INSTR_ID FOREIGN KEY(INSTR_ID) REFERENCES PRODUCTDATA(INSTR_ID)\n"
		+ ") OPTIONS(NAMEINSOURCE '\"PRODUCTS\".\"PRODUCTSYMBOLS\"', UPDATABLE 'FALSE') \n" + "AS\n"
		+ "\t/* first comment in the product symbols*/\n" + "SELECT\n" + "\t\t*\n" + "\tFROM\n"
		+ "\t\t/* second comment in the product symbols*/\n" + "\t\tProducts.PRODUCTSYMBOLS;\n" + "\t\t\n"
		+ "CREATE VIEW TYPETEST (\n"
		+ "\tCUSTOMERID string(12) NOT NULL OPTIONS(NAMEINSOURCE '\"CUSTOMERID\"', NATIVE_TYPE 'VARCHAR2', UPDATABLE 'FALSE'),\n"
		+ "\tDATECOL date NOT NULL OPTIONS(NAMEINSOURCE '\"DATECOL\"', NATIVE_TYPE 'DATE', CASE_SENSITIVE 'FALSE', UPDATABLE 'FALSE', FIXED_LENGTH 'TRUE', SEARCHABLE 'ALL_EXCEPT_LIKE'),\n"
		+ "\tDATETIMECOL timestamp NOT NULL OPTIONS(NAMEINSOURCE '\"DATETIMECOL\"', NATIVE_TYPE 'TIMESTAMP(6)', CASE_SENSITIVE 'FALSE', UPDATABLE 'FALSE', FIXED_LENGTH 'TRUE', SEARCHABLE 'ALL_EXCEPT_LIKE'),\n"
		+ "\tTIMESTAMPWITHTZ timestamp OPTIONS(NAMEINSOURCE '\"TIMESTAMPWITHTZ\"', NATIVE_TYPE 'TIMESTAMP(6) WITH TIME ZONE', CASE_SENSITIVE 'FALSE', UPDATABLE 'FALSE', FIXED_LENGTH 'TRUE', SEARCHABLE 'ALL_EXCEPT_LIKE'),\n"
		+ "\tTIMESTAMP2WITHTZ timestamp OPTIONS(NAMEINSOURCE '\"TIMESTAMP2WITHTZ\"', NATIVE_TYPE 'TIMESTAMP(6) WITH TIME ZONE', CASE_SENSITIVE 'FALSE', UPDATABLE 'FALSE', FIXED_LENGTH 'TRUE', SEARCHABLE 'ALL_EXCEPT_LIKE'),\n"
		+ "\tDOUBLECOL float NOT NULL OPTIONS(NAMEINSOURCE '\"DOUBLECOL\"', NATIVE_TYPE 'FLOAT', CASE_SENSITIVE 'FALSE', UPDATABLE 'FALSE', FIXED_LENGTH 'TRUE', SEARCHABLE 'ALL_EXCEPT_LIKE'),\n"
		+ "\tDECIMAL3COL bigdecimal(3) NOT NULL OPTIONS(NAMEINSOURCE '\"DECIMAL3COL\"', NATIVE_TYPE 'NUMBER', CASE_SENSITIVE 'FALSE', UPDATABLE 'FALSE', FIXED_LENGTH 'TRUE', SEARCHABLE 'ALL_EXCEPT_LIKE'),\n"
		+ "\tDECIMAL22COL bigdecimal(22) NOT NULL OPTIONS(NAMEINSOURCE '\"DECIMAL22COL\"', NATIVE_TYPE 'NUMBER', CASE_SENSITIVE 'FALSE', UPDATABLE 'FALSE', FIXED_LENGTH 'TRUE', SEARCHABLE 'ALL_EXCEPT_LIKE'),\n"
		+ "\tBIGSTRINGCOL string(512) NOT NULL OPTIONS(NAMEINSOURCE '\"BIGSTRINGCOL\"', NATIVE_TYPE 'VARCHAR2', UPDATABLE 'FALSE'),\n"
		+ "\tDECIMAL12COL bigdecimal(12) NOT NULL OPTIONS(NAMEINSOURCE '\"DECIMAL12COL\"', NATIVE_TYPE 'NUMBER', CASE_SENSITIVE 'FALSE', UPDATABLE 'FALSE', FIXED_LENGTH 'TRUE', SEARCHABLE 'ALL_EXCEPT_LIKE'),\n"
		+ "\tINTEGERCOL bigdecimal(22) NOT NULL OPTIONS(NAMEINSOURCE '\"INTEGERCOL\"', NATIVE_TYPE 'NUMBER', CASE_SENSITIVE 'FALSE', UPDATABLE 'FALSE', FIXED_LENGTH 'TRUE', SEARCHABLE 'ALL_EXCEPT_LIKE'),\n"
		+ "\tDECIMAL22D2COL bigdecimal(22, 2) NOT NULL OPTIONS(NAMEINSOURCE '\"DECIMAL22D2COL\"', NATIVE_TYPE 'NUMBER', CASE_SENSITIVE 'FALSE', UPDATABLE 'FALSE', FIXED_LENGTH 'TRUE', SEARCHABLE 'ALL_EXCEPT_LIKE')\n"
		+ ") OPTIONS(NAMEINSOURCE '\"PRODUCTS\".\"TYPETEST\"', UPDATABLE 'FALSE') \n" + "AS\n"
		+ "\t/* first comment in the typetest*/\n" + "SELECT\n" + "\t\t*\n" + "\tFROM\n"
		+ "\t\t/* second comment in the typetest*/\n" + "\t\tProducts.TYPETEST;";
	assertScoreAndParse(content, null, 3);

	{
	    final List<AstNode> kids = getRootNode().childrenWithName("PRODUCTDATA");
	    assertThat(kids.size(), is(1));
	    assertMixinType(kids.get(0), TeiidDdlLexicon.CreateTable.VIEW_STATEMENT);

	    final AstNode viewNode = kids.get(0);
	    assertProperty(viewNode, TeiidDdlLexicon.CreateTable.QUERY_EXPRESSION,
		    "/* first comment in the product data*/ SELECT * FROM /* second comment in the product data*/ Products.PRODUCTDATA");
	}

	{
	    final List<AstNode> kids = getRootNode().childrenWithName("PRODUCTSYMBOLS");
	    assertThat(kids.size(), is(1));
	    assertMixinType(kids.get(0), TeiidDdlLexicon.CreateTable.VIEW_STATEMENT);

	    final AstNode viewNode = kids.get(0);
	    assertProperty(viewNode, TeiidDdlLexicon.CreateTable.QUERY_EXPRESSION,
		    "/* first comment in the product symbols*/ SELECT * FROM /* second comment in the product symbols*/ Products.PRODUCTSYMBOLS");
	}

	{
	    final List<AstNode> kids = getRootNode().childrenWithName("TYPETEST");
	    assertThat(kids.size(), is(1));
	    assertMixinType(kids.get(0), TeiidDdlLexicon.CreateTable.VIEW_STATEMENT);

	    final AstNode viewNode = kids.get(0);
	    assertProperty(viewNode, TeiidDdlLexicon.CreateTable.QUERY_EXPRESSION,
		    "/* first comment in the typetest*/ SELECT * FROM /* second comment in the typetest*/ Products.TYPETEST");
	}
    }

    /**
     * See Teiid TestDDLParser#testMultipleCommands2()
     */
    @Test
    public void shouldParseMultipleCommands2() {
        final String content = "CREATE VIRTUAL PROCEDURE getTweets(query varchar) RETURNS (created_on varchar(25), "
                               + "from_user varchar(25), to_user varchar(25), "
                               + "profile_image_url varchar(25), source varchar(25), text varchar(140)) AS "
                               + "select tweet.* from "
                               + "(call twitter.invokeHTTP(action => 'GET', endpoint =>querystring('',query as \"q\"))) w, "
                               + "XMLTABLE('results' passing JSONTOXML('myxml', w.result) columns "
                               + "created_on string PATH 'created_at', " + "from_user string PATH 'from_user', "
                               + "to_user string PATH 'to_user', " + "profile_image_url string PATH 'profile_image_url', "
                               + "source string PATH 'source', " + "text string PATH 'text') tweet;"
                               + "CREATE VIEW Tweet AS select * FROM twitterview.getTweets;";
        assertScoreAndParse(content, null, 2);

        {
            final List<AstNode> kids = getRootNode().childrenWithName("getTweets");
            assertThat(kids.size(), is(1));
            assertMixinType(kids.get(0), TeiidDdlLexicon.CreateProcedure.PROCEDURE_STATEMENT);
        }

        {
            final List<AstNode> kids = getRootNode().childrenWithName("Tweet");
            assertThat(kids.size(), is(1));
            assertMixinType(kids.get(0), TeiidDdlLexicon.CreateTable.VIEW_STATEMENT);
        }
    }

    /**
     * See Teiid TestDDLParser#testInsteadOfTrigger()
     */
    @Test
    public void shouldParseInsteadOfTrigger() {
        final String content = "CREATE VIEW G1( e1 integer, e2 varchar) AS select * from foo;"
                               + "CREATE TRIGGER ON G1 INSTEAD OF INSERT AS " + "FOR EACH ROW " + "BEGIN ATOMIC "
                               + "insert into g1 (e1, e2) values (1, 'trig');" + "END;"
                               + "CREATE View G2( e1 integer, e2 varchar) AS select * from foo;";
        assertScoreAndParse(content, null, 3);

        {
            final List<AstNode> kids = getRootNode().childrenWithName("G1");
            assertThat(kids.size(), is(2)); // view, trigger
            assertMixinType(kids.get(0), TeiidDdlLexicon.CreateTable.VIEW_STATEMENT);
            assertMixinType(kids.get(1), TeiidDdlLexicon.CreateTrigger.STATEMENT);
        }

        {
            final List<AstNode> kids = getRootNode().childrenWithName("G2");
            assertThat(kids.size(), is(1));
            assertMixinType(kids.get(0), TeiidDdlLexicon.CreateTable.VIEW_STATEMENT);
        }
    }

    /**
     * See Teiid TestDDLParser#testFK()
     */
    @Test
    public void shouldParseForeignKey() {
        final String content = "CREATE FOREIGN TABLE G1(g1e1 integer, g1e2 varchar, PRIMARY KEY(g1e1, g1e2));"
                               + "CREATE FOREIGN TABLE G2( g2e1 integer, g2e2 varchar, FOREIGN KEY (g2e1, g2e2) REFERENCES G1 (g1e1, g1e2))";
        assertScoreAndParse(content, null, 2);

        {
            final List<AstNode> kids = getRootNode().childrenWithName("G1");
            assertThat(kids.size(), is(1));
            assertMixinType(kids.get(0), TeiidDdlLexicon.CreateTable.TABLE_STATEMENT);
        }

        {
            final List<AstNode> kids = getRootNode().childrenWithName("G2");
            assertThat(kids.size(), is(1));
            assertMixinType(kids.get(0), TeiidDdlLexicon.CreateTable.TABLE_STATEMENT);
        }
    }

    /**
     * See Teiid TestDDLParser#testOptionalFK()
     */
    @Test
    public void shouldParseOptionalForeignKey() {
        final String content = "CREATE FOREIGN TABLE G1(g1e1 integer, g1e2 varchar, PRIMARY KEY(g1e1, g1e2));"
                               + "CREATE FOREIGN TABLE G2( g2e1 integer, g2e2 varchar, PRIMARY KEY(g2e1, g2e2), FOREIGN KEY (g2e1, g2e2) REFERENCES G1)";
        assertScoreAndParse(content, null, 2);

        {
            final List<AstNode> kids = getRootNode().childrenWithName("G1");
            assertThat(kids.size(), is(1));
            assertMixinType(kids.get(0), TeiidDdlLexicon.CreateTable.TABLE_STATEMENT);
        }

        {
            final List<AstNode> kids = getRootNode().childrenWithName("G2");
            assertThat(kids.size(), is(1));
            assertMixinType(kids.get(0), TeiidDdlLexicon.CreateTable.TABLE_STATEMENT);
        }
    }

    @Test
    public void shouldParseAccountsDdl() {
        final String content = "CREATE FOREIGN TABLE \"accounts.ACCOUNT\" ("
                               + "  ACCOUNT_ID long NOT NULL DEFAULT '0' OPTIONS (ANNOTATION '', NAMEINSOURCE '`ACCOUNT_ID`', NATIVE_TYPE 'INT'),"
                               + "  SSN string(10) OPTIONS (ANNOTATION '', NAMEINSOURCE '`SSN`', NATIVE_TYPE 'CHAR'),"
                               + "  STATUS string(10) OPTIONS (ANNOTATION '', NAMEINSOURCE '`STATUS`', NATIVE_TYPE 'CHAR'),"
                               + "  TYPE string(10) OPTIONS (ANNOTATION '', NAMEINSOURCE '`TYPE`', NATIVE_TYPE 'CHAR'),"
                               + "  DATEOPENED timestamp NOT NULL DEFAULT 'CURRENT_TIMESTAMP' OPTIONS (ANNOTATION '', NAMEINSOURCE '`DATEOPENED`', NATIVE_TYPE 'TIMESTAMP'),"
                               + "  DATECLOSED timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' OPTIONS (ANNOTATION '', NAMEINSOURCE '`DATECLOSED`', NATIVE_TYPE 'TIMESTAMP')"
                               + "  )" + " OPTIONS (ANNOTATION '', NAMEINSOURCE '`accounts`.`ACCOUNT`', UPDATABLE TRUE);";
        assertScoreAndParse(content, null, 1);
        final AstNode tableNode = getRootNode().getChildren().get(0);
        assertMixinType(tableNode, TeiidDdlLexicon.CreateTable.TABLE_STATEMENT);
        assertProperty(tableNode, TeiidDdlLexicon.SchemaElement.TYPE, SchemaElementType.FOREIGN.toDdl());
    }

    @Test
    public void shouldParsePortfolioBR() {
        final String content = "CREATE VIRTUAL FUNCTION performRuleOnData(className string, returnMethodName string, returnIfNull string, VARIADIC z object )"
                               + "RETURNS string OPTIONS (JAVA_CLASS 'org.jboss.teiid.businessrules.udf.RulesUDF',  JAVA_METHOD 'performRuleOnData', VARARGS 'true');"
                               + "CREATE VIEW StockPrices ("
                               + "  symbol string,"
                               + "  price bigdecimal"
                               + "  )"
                               + "  AS "
                               + "    SELECT StockPrices.symbol, StockPrices.price"
                               + "      FROM (EXEC MarketData.getTextFiles('*.txt')) AS f,"
                               + "        TEXTTABLE(f.file COLUMNS symbol string, price bigdecimal HEADER) AS StockPrices;"
                               + "CREATE VIEW Stock ("
                               + "  symbol string,"
                               + "  price bigdecimal,"
                               + "  company_name   varchar(256)"
                               + "  )"
                               + "  AS"
                               + "    SELECT  S.symbol, S.price, A.COMPANY_NAME"
                               + "      FROM StockPrices AS S, Accounts.PRODUCT AS A"
                               + "      WHERE S.symbol = A.SYMBOL;"
                               + "CREATE VIRTUAL PROCEDURE StockValidation() RETURNS (companyname varchar(256), symbol string(10), price bigdecimal, message varchar(256) )"
                               + "  AS"
                               + "    BEGIN"
                               + "      DECLARE String VARIABLES.msg;"
                               + "      CREATE LOCAL TEMPORARY TABLE TEMP (companyname string, symbol string, price bigdecimal, message string);"
                               + "      LOOP ON (SELECT Stock.symbol, Stock.price, Stock.company_name FROM Stock) AS txncursor"
                               + "      BEGIN"
                               + "        VARIABLES.msg = Stocks.performRuleOnData('org.jboss.teiid.quickstart.data.MarketData', 'getInvalidMessage', 'noMsg', txncursor.company_name, txncursor.symbol, txncursor.price);"
                               + "        IF(VARIABLES.msg <> 'NoMsg')"
                               + "          BEGIN"
                               + "            INSERT INTO TEMP (TEMP.companyname, TEMP.symbol, TEMP.price, TEMP.message) VALUES (txncursor.COMPANY_NAME, txncursor.symbol, txncursor.price, VARIABLES.msg);"
                               + "          END"
                               + "      END"
                               + "      SELECT TEMP.companyname, TEMP.symbol, TEMP.price, TEMP.message FROM TEMP;"
                               + "    END"
                               + "";

        assertScoreAndParse(content, null, 4);

        {
            final List<AstNode> kids = getRootNode().childrenWithName("performRuleOnData");
            assertThat(kids.size(), is(1));
            assertMixinType(kids.get(0), TeiidDdlLexicon.CreateProcedure.FUNCTION_STATEMENT);
        }

        {
            final List<AstNode> kids = getRootNode().childrenWithName("StockPrices");
            assertThat(kids.size(), is(1));
            assertMixinType(kids.get(0), TeiidDdlLexicon.CreateTable.VIEW_STATEMENT);
        }

        {
            final List<AstNode> kids = getRootNode().childrenWithName("Stock");
            assertThat(kids.size(), is(1));
            assertMixinType(kids.get(0), TeiidDdlLexicon.CreateTable.VIEW_STATEMENT);
        }

        {
            final List<AstNode> kids = getRootNode().childrenWithName("StockValidation");
            assertThat(kids.size(), is(1));
            assertMixinType(kids.get(0), TeiidDdlLexicon.CreateProcedure.PROCEDURE_STATEMENT);
        }
    }

    @Test
    public void shouldParseAndResolveTableReference() {
        final String content = "CREATE FOREIGN TABLE G2(g2e1 integer, g2e2 varchar, PRIMARY KEY(g2e1, g2e2), FOREIGN KEY (g2e1, g2e2) REFERENCES G1)"
                               + "CREATE FOREIGN TABLE G1(g1e1 integer, g1e2 varchar, PRIMARY KEY(g1e1, g1e2));";

        assertScoreAndParse(content, null, 2);

        {
            final List<AstNode> kids = getRootNode().childrenWithName("G1");
            assertThat(kids.size(), is(1));
            assertMixinType(kids.get(0), TeiidDdlLexicon.CreateTable.TABLE_STATEMENT);
        }

        {
            final List<AstNode> kids = getRootNode().childrenWithName("G2");
            assertThat(kids.size(), is(1));
            assertMixinType(kids.get(0), TeiidDdlLexicon.CreateTable.TABLE_STATEMENT);
        }
    }

    @Test
    public void shouldParseTeiidStatements_1() {
        // Parses a simplified DDL that contains 3 tables, 2 with Foreign keys.
        // The Tables in the DDL are arranged, such that the first table has FK reference to 3rd table
        // and results in an Unresolved table reference that should be handled by a postProcess() method

        printTest("shouldParseTeiidStatements_1()");
        String content = getFileContent(DDL_FILE_PATH + "sap_short_test.ddl");
        assertScoreAndParse(content, "teiid_test_statements_1", 3);
        final AstNode tableNode = getRootNode().getChildren().get(0);
        if (tableNode != null) {
            final List<AstNode> kids = getRootNode().childrenWithName("BookingCollection");
            assertThat(kids.size(), is(1));
            final List<AstNode> tableKids = kids.get(0).getChildren();
            assertThat(tableKids.size(), is(9));
            final List<AstNode> fkNodes = kids.get(0).childrenWithName("BookingFlight");
            assertThat(fkNodes.size(), is(1));

            final List<AstNode> fc_kids = getRootNode().childrenWithName("FlightCollection");
            assertThat(fc_kids.size(), is(1));
            final List<AstNode> fc_columns = fc_kids.get(0).childrenWithName("carrid");
            assertThat(fc_columns.size(), is(1));
            AstNode columnNode = fc_columns.get(0);

            @SuppressWarnings( "unchecked" )
            ArrayList<AstNode> props = ((ArrayList<AstNode>)fkNodes.get(0).getProperty(TeiidDdlLexicon.Constraint.TABLE_REFERENCE_REFERENCES));
            AstNode refColumnNode = props.get(0);
            assertThat(refColumnNode, is(columnNode));
        }
    }

    @Test
    public void shouldParseTeiidStatements_2() {
        // Parses a full DDL file that contains multiple tables with multiple FK's
        // The Tables in the DDL are arranged, such that the at least one table has FK reference to table defined later in the DDL
        // and results in an Unresolved table reference that should be handled by a postProcess() method

        printTest("shouldParseTeiidStatements_2()");
        String content = getFileContent(DDL_FILE_PATH + "sap-flight.ddl");
        assertScoreAndParse(content, "teiid_test_statements_2", 12);
        final AstNode tableNode = getRootNode().getChildren().get(0);

        if (tableNode != null) {
            final List<AstNode> kids = getRootNode().childrenWithName("BookingCollection");
            assertThat(kids.size(), is(1));
            final List<AstNode> tableKids = kids.get(0).getChildren();
            assertThat(tableKids.size(), is(28));
            final List<AstNode> fkNodes = kids.get(0).childrenWithName("BookingFlight");
            assertThat(fkNodes.size(), is(1));

            final List<AstNode> fc_kids = getRootNode().childrenWithName("FlightCollection");
            assertThat(fc_kids.size(), is(1));
            final List<AstNode> fc_columns = fc_kids.get(0).childrenWithName("carrid");
            assertThat(fc_columns.size(), is(1));
            AstNode columnNode = fc_columns.get(0);

            @SuppressWarnings( "unchecked" )
            ArrayList<AstNode> props = ((ArrayList<AstNode>)fkNodes.get(0).getProperty(TeiidDdlLexicon.Constraint.TABLE_REFERENCE_REFERENCES));
            AstNode refColumnNode = null;
            for (AstNode nextColumnNode : props) {

                if (nextColumnNode.getName().equals("carrid")) {
                    refColumnNode = nextColumnNode;
                    break;
                }
            }

            assertThat(refColumnNode, is(columnNode));
        }
    }

    @Test
    public void shouldParseHanaDdl() {
        // the HANA DDL uncovered 2 problems:
        // (1) a column called INDEX was being parsed as a constraint instead of a column definition, and
        // (2) the tokenizer was not parsing embedded quotes correctly (2 adjacent quotes are considered one embedded quote)
        printTest("shouldSapHana()");

        String content = getFileContent(DDL_FILE_PATH + "sap-hana.ddl");
        assertScoreAndParse(content, "teiid_test_statements_2", 5); // 2 tablse + 3 ignorable comments 
        
        { // test for parsing column named INDEX
            final List<AstNode> tables = getRootNode().childrenWithName("_SYS_STATISTICS.GLOBAL_COLUMN_TABLES_SIZE");
            assertThat(tables.size(), is(1));

            final AstNode table = tables.get(0);
            final String columnName = "INDEX";
            assertThat(table.childrenWithName(columnName).size(), is(1));

            final AstNode column = table.childrenWithName(columnName).get(0);
            assertThat(column.hasMixin(TeiidDdlLexicon.CreateTable.TABLE_ELEMENT), is(true));
        }

        { // test embedded quotes
            final List<AstNode> tables = getRootNode().childrenWithName("PUBLIC.AUDIT_POLICIES");
            assertThat(tables.size(), is(1));

            final AstNode table = tables.get(0);
            final String columnName = "EVENT_STATUS";
            assertThat(table.childrenWithName(columnName).size(), is(1));

            final AstNode column = table.childrenWithName(columnName).get(0);
            final String optionName = "ANNOTATION";
            assertThat(column.childrenWithName(optionName).size(), is(1));

            final AstNode option = column.childrenWithName(optionName).get(0);
            assertThat(option.getProperty(StandardDdlLexicon.VALUE).toString(),
                       is("Status of events to be audited: ''SUCCESSFUL EVENTS''/''UNSUCCESSFUL EVENTS''/''ALL EVENTS''"));
        }
    }

}
