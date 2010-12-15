package org.drools.camel.poc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.naming.Context;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.util.jndi.JndiContext;
import org.drools.KnowledgeBase;
import org.drools.KnowledgeBaseFactory;
import org.drools.base.MapGlobalResolver;
import org.drools.command.BatchExecutionCommand;
import org.drools.command.CommandFactory;
import org.drools.command.impl.GenericCommand;
import org.drools.command.runtime.process.SignalEventCommand;
import org.drools.command.runtime.process.StartProcessCommand;
import org.drools.common.AbstractRuleBase;
import org.drools.compiler.ProcessBuilderFactory;
import org.drools.grid.GridNode;
import org.drools.grid.impl.GridImpl;
import org.drools.grid.service.directory.WhitePages;
import org.drools.grid.service.directory.impl.WhitePagesImpl;
import org.drools.impl.InternalKnowledgeBase;
import org.drools.marshalling.impl.ProcessMarshallerFactory;
import org.drools.persistence.jpa.JPAKnowledgeService;
import org.drools.runtime.Environment;
import org.drools.runtime.EnvironmentName;
import org.drools.runtime.ExecutionResults;
import org.drools.runtime.StatefulKnowledgeSession;
import org.drools.runtime.process.ProcessContext;
import org.drools.runtime.process.ProcessInstance;
import org.drools.runtime.process.ProcessRuntimeFactory;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Server;
import org.jbpm.marshalling.impl.ProcessMarshallerFactoryServiceImpl;
import org.jbpm.process.builder.ProcessBuilderFactoryServiceImpl;
import org.jbpm.process.core.event.EventTypeFilter;
import org.jbpm.process.instance.ProcessRuntimeFactoryServiceImpl;
import org.jbpm.process.instance.impl.Action;
import org.jbpm.ruleflow.core.RuleFlowProcess;
import org.jbpm.ruleflow.instance.RuleFlowProcessInstance;
import org.jbpm.workflow.core.Node;
import org.jbpm.workflow.core.impl.ConnectionImpl;
import org.jbpm.workflow.core.impl.DroolsConsequenceAction;
import org.jbpm.workflow.core.node.ActionNode;
import org.jbpm.workflow.core.node.EndNode;
import org.jbpm.workflow.core.node.EventNode;
import org.jbpm.workflow.core.node.StartNode;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import bitronix.tm.BitronixTransactionManager;
import bitronix.tm.TransactionManagerServices;
import bitronix.tm.resource.jdbc.PoolingDataSource;

public class JPAFlowCamelIntegrationTest {

    private static Server h2Server;
    private PoolingDataSource ds1;
    private EntityManagerFactory emf;

    @BeforeClass
    public static void startH2Database(){
        ProcessBuilderFactory.setProcessBuilderFactoryService(new ProcessBuilderFactoryServiceImpl());
        ProcessMarshallerFactory.setProcessMarshallerFactoryService(new ProcessMarshallerFactoryServiceImpl());
        ProcessRuntimeFactory.setProcessRuntimeFactoryService(new ProcessRuntimeFactoryServiceImpl());
        try {
            DeleteDbFiles.execute("", "JPADroolsFlow", true);
            h2Server = Server.createTcpServer(new String[0]);
            h2Server.start();
        } catch (SQLException e) {
            throw new RuntimeException("can't start h2 server db",e);
        }
    }

    @AfterClass
    public static void stopH2Database() throws Throwable {
        if (h2Server != null) {
            h2Server.stop();
        }
        DeleteDbFiles.execute("", "JPADroolsFlow", true);
    }

    @Before
    public void setUp() {
        ds1 = new PoolingDataSource();
        ds1.setUniqueName("jdbc/testDS1");
        ds1.setMaxPoolSize(5);
        ds1.setAllowLocalTransactions(true);

        ds1.setClassName( "org.h2.jdbcx.JdbcDataSource" );
        ds1.setMaxPoolSize( 3 );
        ds1.getDriverProperties().put( "user",
        "sa" );
        ds1.getDriverProperties().put( "password",
        "sasa" );
        ds1.getDriverProperties().put( "URL",
        "jdbc:h2:mem:" );

        ds1.init();
        emf = Persistence.createEntityManagerFactory( "org.drools.persistence.jpa" );
    }

    @After
    public void tearDown() {
        emf.close();
        ds1.close();
    }

    @Test
    public void multipleProcessWithSignalEvent() {
        
        String processId = "signalProcessTest";
        String eventType = "myEvent";
        RuleFlowProcess process = newSimpleEventProcess(processId, eventType);
        
        KnowledgeBase kbase = KnowledgeBaseFactory.newKnowledgeBase();
        ((AbstractRuleBase) ((InternalKnowledgeBase) kbase).getRuleBase()).addProcess(process);

        StatefulKnowledgeSession ksession = JPAKnowledgeService.newStatefulKnowledgeSession(kbase, null, createEnvironment());
        ProcessInstance processInstance1 = ksession.startProcess(processId);
        long processInstance1Id = processInstance1.getId();
        Assert.assertEquals(ProcessInstance.STATE_ACTIVE, processInstance1.getState());
        ProcessInstance processInstance2 = ksession.startProcess(processId);
        long processInstance2Id = processInstance2.getId();
        Assert.assertEquals(ProcessInstance.STATE_ACTIVE, processInstance2.getState());

        int sessionId = ksession.getId();
        ksession.dispose();
        ksession = JPAKnowledgeService.loadStatefulKnowledgeSession(sessionId, kbase, null, createEnvironment());
        ksession.signalEvent(eventType, null, processInstance1Id);
        processInstance1 = (RuleFlowProcessInstance) ksession.getProcessInstance(processInstance1Id);
        processInstance2 = (RuleFlowProcessInstance) ksession.getProcessInstance(processInstance2Id);

        Assert.assertNull(processInstance1);
        Assert.assertEquals(ProcessInstance.STATE_ACTIVE, processInstance2.getState());
    }

    @Test
    public void simpleCamelIntegration() throws Exception {

        String processId = "signalProcessTest";

        KnowledgeBase kbase = KnowledgeBaseFactory.newKnowledgeBase();
        ((AbstractRuleBase) ((InternalKnowledgeBase) kbase).getRuleBase()).addProcess(newProcessWithScriptTask(processId, new Action() {
            
            @Override
            public void execute(ProcessContext context) throws Exception {
                System.out.println("Script task node executed");
                
            }
        }));
        
        StatefulKnowledgeSession ksession = kbase.newStatefulKnowledgeSession();

        Context context = configureGridContext(ksession);

        CamelContext camelContext = createCamelContext(context);
        camelContext.start();

        StartProcessCommand startProcessCommand = new StartProcessCommand(processId, "process-instance-id");
        List<GenericCommand<?>> commands = new ArrayList<GenericCommand<?>>();
        commands.add(startProcessCommand);
        BatchExecutionCommand batchExecutionCommand = CommandFactory.newBatchExecution(commands, "ksession1");

        ProducerTemplate template = camelContext.createProducerTemplate();
        ExecutionResults response = (ExecutionResults) template.requestBody("direct:test-with-session", batchExecutionCommand);
        Assert.assertNotNull(response);
        Long processInstanceId = (Long) response.getValue("process-instance-id");
        Assert.assertNotNull(processInstanceId);
    }
    
    private Context configureGridContext(StatefulKnowledgeSession ksession) throws Exception {
        GridImpl grid = new GridImpl();
        grid.addService(WhitePages.class, new WhitePagesImpl());
        GridNode node = grid.createGridNode("node");
        Context context = new JndiContext();
        context.bind("node", node);
        node.set("ksession1", ksession);
        return context;
    }
    
    private CamelContext createCamelContext(Context context) throws Exception {
        CamelContext camelContext = new DefaultCamelContext(context);
        RouteBuilder rb = new RouteBuilder() {
            public void configure() throws Exception {
                from("direct:test-with-session").to("drools://node/ksession1");
            }
        };
        camelContext.addRoutes(rb);
        return camelContext;
    }
    
    @Test
    public void jpaCamelMultipleProcessWithSignalEvent() throws Exception {

        String processId = "signalProcessTest";
        String eventType = "myEvent";

        KnowledgeBase kbase = KnowledgeBaseFactory.newKnowledgeBase();
        ((AbstractRuleBase) ((InternalKnowledgeBase) kbase).getRuleBase()).addProcess(newSimpleEventProcess(processId, eventType));

        StatefulKnowledgeSession ksession = JPAKnowledgeService.newStatefulKnowledgeSession(kbase, null, createEnvironment());

        Context context = configureGridContext(ksession);
        CamelContext camelContext = createCamelContext(context);
        camelContext.start();

        StartProcessCommand startProcess1Command = new StartProcessCommand(processId, "first-instance");
        StartProcessCommand startProcess2Command = new StartProcessCommand(processId, "second-instance");
        List<GenericCommand<?>> commands = new ArrayList<GenericCommand<?>>();
        commands.add(startProcess1Command);
        commands.add(startProcess2Command);
        BatchExecutionCommand batchExecutionCommand = CommandFactory.newBatchExecution(commands, "ksession1");

        ProducerTemplate template = camelContext.createProducerTemplate();
        ExecutionResults response = (ExecutionResults) template.requestBody("direct:test-with-session", batchExecutionCommand);
        Long processInstance1Id = (Long) response.getValue("first-instance");
        Long processInstance2Id = (Long) response.getValue("second-instance");
        Assert.assertNotNull(response);
        Assert.assertNotNull(processInstance1Id);
        Assert.assertNotNull(processInstance2Id);

        int sessionId = ksession.getId();

        ksession.dispose();
        camelContext.stop();

        ksession = JPAKnowledgeService.loadStatefulKnowledgeSession(sessionId, kbase, null, createEnvironment());
        context = configureGridContext(ksession);
        camelContext = createCamelContext(context);
        camelContext.start();

        SignalEventCommand signalEventCommand = new SignalEventCommand(processInstance1Id, eventType, null);
        commands = new ArrayList<GenericCommand<?>>();
        commands.add(signalEventCommand);
        batchExecutionCommand = CommandFactory.newBatchExecution(commands, "ksession1");

        template = camelContext.createProducerTemplate();
        response = (ExecutionResults) template.requestBody("direct:test-with-session", batchExecutionCommand);

        RuleFlowProcessInstance processInstance1 = (RuleFlowProcessInstance) ksession.getProcessInstance(processInstance1Id);
        RuleFlowProcessInstance processInstance2 = (RuleFlowProcessInstance) ksession.getProcessInstance(processInstance2Id);

        ksession.dispose();
        camelContext.stop();

        Assert.assertNull(processInstance1);
        Assert.assertEquals(ProcessInstance.STATE_ACTIVE, processInstance2.getState());
    }

    @Test
    public void jpaSimpleCamelIntegration() throws Exception {
        GridImpl grid = new GridImpl();
        grid.addService(WhitePages.class, new WhitePagesImpl());
        GridNode node = grid.createGridNode("node");
        Context context = new JndiContext();
        context.bind("node", node);

        String processId = "signalProcessTest";

        KnowledgeBase kbase = KnowledgeBaseFactory.newKnowledgeBase();
        ((AbstractRuleBase) ((InternalKnowledgeBase) kbase).getRuleBase()).addProcess(newProcessWithScriptTask(processId, new Action() {
            @Override
            public void execute(ProcessContext context) throws Exception {
                System.out.println("Script task node executed");
            }
        }));

        StatefulKnowledgeSession ksession = JPAKnowledgeService.newStatefulKnowledgeSession(kbase, null, createEnvironment());
        node.set("ksession1", ksession);

        CamelContext camelContext = new DefaultCamelContext(context);
        RouteBuilder rb = new RouteBuilder() {
            public void configure() throws Exception {
                from("direct:test-with-session").to("drools://node/ksession1");
            }
        };
        camelContext.addRoutes(rb);
        camelContext.start();

        StartProcessCommand startProcessCommand = new StartProcessCommand(processId, "processInstanceID");
        List<GenericCommand<?>> commands = new ArrayList<GenericCommand<?>>();
        commands.add(startProcessCommand);
        BatchExecutionCommand batchExecutionCommand = CommandFactory.newBatchExecution(commands, "ksession1");

        ProducerTemplate template = camelContext.createProducerTemplate();
        ExecutionResults response = (ExecutionResults) template.requestBody("direct:test-with-session", batchExecutionCommand);
        Assert.assertNotNull(response);
        Assert.assertNotNull(response.getValue("processInstanceID"));
    }

    private RuleFlowProcess newSimpleEventProcess(String processId, String eventType) {
        RuleFlowProcess process = new RuleFlowProcess();
        process.setId(processId);

        StartNode startNode = new StartNode();
        startNode.setName("Start");
        startNode.setId(1);

        EventNode eventNode = new EventNode();
        eventNode.setName("EventNode");
        eventNode.setId(2);
        eventNode.setScope("external");
        EventTypeFilter eventFilter = new EventTypeFilter();
        eventFilter.setType(eventType);
        eventNode.addEventFilter(eventFilter);

        EndNode endNode = new EndNode();
        endNode.setName("End");
        endNode.setId(3);

        connect(startNode, eventNode);
        connect(eventNode, endNode);

        process.addNode(startNode);
        process.addNode(eventNode);
        process.addNode(endNode);
        return process;
    }

    public RuleFlowProcess newProcessWithScriptTask(String processId, Action action) {
        StartNode startNode = new StartNode();
        startNode.setName("Start");
        startNode.setId(1);
        EndNode endNode = new EndNode();
        endNode.setName("EndNode");
        endNode.setId(2);

        ActionNode actionNode = new ActionNode();
        actionNode.setId(3);
        DroolsConsequenceAction insertAction = new DroolsConsequenceAction("java", null);
        insertAction.setMetaData("Action", action);
        actionNode.setAction(insertAction);

        connect(startNode, actionNode);
        connect(actionNode, endNode);

        RuleFlowProcess process = new RuleFlowProcess();
        process.setId(processId);
        process.addNode(startNode);
        process.addNode(actionNode);
        process.addNode(endNode);
        return process;
    }

    private void connect(Node sourceNode, Node targetNode) {
        new ConnectionImpl(sourceNode, Node.CONNECTION_DEFAULT_TYPE,
                targetNode, Node.CONNECTION_DEFAULT_TYPE);
    }

    private Environment createEnvironment() {
        Environment env = KnowledgeBaseFactory.newEnvironment();
        env.set( EnvironmentName.ENTITY_MANAGER_FACTORY, emf );
        BitronixTransactionManager transactionManager = TransactionManagerServices.getTransactionManager();
        env.set( EnvironmentName.TRANSACTION_MANAGER, transactionManager );
        env.set(EnvironmentName.GLOBALS, new MapGlobalResolver());
        return env;
    }

}
