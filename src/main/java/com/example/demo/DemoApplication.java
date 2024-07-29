package com.example.demo;

import jakarta.annotation.PreDestroy;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.context.event.EventListener;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;
import sun.misc.Signal;
import sun.misc.SignalHandler;

@SpringBootApplication
@EnableKafka
public class DemoApplication {


	private static boolean isRunning = true;

	@Autowired
	KafkaListenerControlService kafkaListenerControlService;

	@Autowired
	private ApplicationContext applicationContext;

	@Autowired
	private TaskExecutor taskExecutor;


	@EventListener(ApplicationReadyEvent.class)
	public void doSomethingAfterStartup() {
		System.out.println("Starting top priority listener");
		kafkaListenerControlService.startListener(Constants.CRITICAL_LISTENER_ID);

		System.out.println("Starting high priority listener");
		kafkaListenerControlService.startListener(Constants.HIGH_LISTENER_ID);

		System.out.println("Pause high priority listener");
		kafkaListenerControlService.pauseListener(Constants.HIGH_LISTENER_ID);

		final MessageListenerContainer listenerContainer  = kafkaListenerControlService.getListenerContainer(Constants.CRITICAL_LISTENER_ID);
		while (!listenerContainer.isRunning()) {
			try {
				Thread.sleep(1000);
			}
			catch (InterruptedException ex) {
			}
		}

		ControlTask taskToRun = applicationContext.getBean(ControlTask.class);
		taskExecutor.execute(taskToRun);
	}
	@Component
	public static class ControlTask implements Runnable {
		@Autowired
		KafkaListenerControlService kafkaListenerControlService;

		@Override
		public void run() {

			runInBackgorund();
		}

		protected void runInBackgorund() {

			final MessageListenerContainer criticalContainer  = kafkaListenerControlService.getListenerContainer(Constants.CRITICAL_LISTENER_ID);
			final MessageListenerContainer highContainer  = kafkaListenerControlService.getListenerContainer(Constants.HIGH_LISTENER_ID);

			while (isRunning) {
				try {
					Thread.sleep(3000);
					if (!criticalContainer.isContainerPaused()) {
						// No data in 10 seconds for the critical consumer
						if (System.currentTimeMillis() -
								(CriticalPriorityListener.getLastPollTimeMs() == 0 ? System.currentTimeMillis() :
										CriticalPriorityListener.getLastPollTimeMs()) > 10000L) {
							System.out.println("Pause critical consumer");
							criticalContainer.pause();
							while (!criticalContainer.isContainerPaused()) {
								try {
									Thread.sleep(1000);
								}
								catch (InterruptedException ex) {
								}
							}
							System.out.println("Resume high consumer");
							highContainer.resume();
							while (!highContainer.isRunning()) {
								try {
									Thread.sleep(1000);
								}
								catch (InterruptedException ex) {
								}
							}

						}
					}
					else if (!highContainer.isContainerPaused()) {
						if (System.currentTimeMillis() -
								(HighPriorityListener.getLastPollTimeMs() == 0 ? System.currentTimeMillis() :
										HighPriorityListener.getLastPollTimeMs()) > 10000L) {
							System.out.println("Pause high consumer");
							highContainer.pause();
							while (!highContainer.isContainerPaused()) {
								try {
									Thread.sleep(1000);
								}
								catch (InterruptedException ex) {
								}
							}
							System.out.println("Resume critical consumer");
							criticalContainer.resume();
							while (!criticalContainer.isRunning()) {
								try {
									Thread.sleep(1000);
								}
								catch (InterruptedException ex) {
								}
							}

						}
					}

					System.out.println("Running in background");
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}


	public static void main(String[] args) {



		// addShutdownHook will catch System.exit command of the signal handler.
		// If you have your own shutdown process, you can add it to this shutdown hook.
//		Runtime.getRuntime().addShutdownHook(
//				new Thread(() -> System.out.println("Gracefully shoutdown process is started!")));

		ConfigurableApplicationContext appCtx = SpringApplication.run(DemoApplication.class, args);

		addSignalHandler(appCtx);
	}

	private static void addSignalHandler(ConfigurableApplicationContext appCtx) {
		SignalHandler signalHandler = new SignalHandlerImpl(appCtx);
		Signal.handle(new Signal("TERM"), signalHandler);
		Signal.handle(new Signal("INT"), signalHandler);
	}

	private static class SignalHandlerImpl implements SignalHandler {

		private ConfigurableApplicationContext appCtx = null;

		public SignalHandlerImpl(ConfigurableApplicationContext appCtx) {

			this.appCtx = appCtx;
		}
		@Override
		public void handle(Signal signal) {
			DemoApplication.isRunning = false;
			System.out.println("Handling signal" + signal.getName());
			appCtx.close();
		}
	}

}
