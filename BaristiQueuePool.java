package de.e_nexus.web.rm.db.jdbc;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class BaristiQueuePool {

	private boolean open = true;

	private int workCount = 0;

	private int finishedThreads = 0;
	private Object lock = new Object();

	private class Barista extends Thread {
		private Set<Runnable> work = new LinkedHashSet<>();

		@Override
		public void run() {
			try {
				while (open) {
					if (work.size() == 0) {
						sleep(100);
					} else {
						synchronized (work) {
							Iterator<Runnable> iterator = work.iterator();
							if (iterator.hasNext()) {
								Runnable next = iterator.next();
								work.remove(next);
								try {
									next.run();
								} catch (Exception e) {
									e.printStackTrace();
								}
								decreateWorkCount();
							}
						}
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			increaseFinishedThreads();
		}

		public void addWork(Runnable work) {
			workCount++;
			this.work.add(work);
		}

		public int getWorkSize() {
			return work.size();
		}

		@Override
		public void destroy() {
			super.destroy();
		}
	}

	private Barista[] baristi;

	private Runnable emptyNotifier;

	public BaristiQueuePool(byte baristaCount, String name, Runnable emptyNotifier) {
		this.emptyNotifier = emptyNotifier;
		baristi = new Barista[baristaCount];
		for (int i = 0; i < baristi.length; i++) {
			baristi[i] = new Barista();
			baristi[i].setName(name + "-" + (1 + i) + "/" + baristaCount);
		}
	}

	public void increaseFinishedThreads() {
		finishedThreads++;
		if (finishedThreads == baristi.length) {
			System.out.println("Stopped");
			emptyNotifier.run();
		}

	}

	public void decreateWorkCount() {
		boolean mustQuit = false;
		synchronized (this) {
			workCount--;
			if (workCount == 0) {
				mustQuit = true;
			}
		}
		if (mustQuit) {
			open = false;
			synchronized (lock) {
				lock.notifyAll();
			}
		}
	}

	public void execute(Runnable work) {
		for (Barista barista : baristi) {
			if (barista.getWorkSize() == 0) {
				barista.addWork(work);
				System.out.println("Barista " + barista + " had 0");
				return;
			}
		}
		int minimumWorks = Integer.MAX_VALUE;
		Barista minimumWorkBarista = null;
		for (Barista barista : baristi) {
			int currentWorkSize = barista.getWorkSize();
			if (currentWorkSize < minimumWorks) {
				minimumWorks = currentWorkSize;
				minimumWorkBarista = barista;
			}
		}
		minimumWorkBarista.addWork(work);
	}

	public boolean awaitTermination(int replicationTimeout, TimeUnit minutes) {
		if (workCount > 0) {

			for (Barista barista : baristi) {
				barista.start();
			}
			try {
				Thread t = new Thread("Pool killer " + replicationTimeout + minutes.toString()) {
					@Override
					public void run() {
						try {
							long df = minutes.toMillis(replicationTimeout);
							System.out.println(df);
							Thread.currentThread().sleep(df);
							System.out.println("KILL");
							synchronized (lock) {
								lock.notifyAll();
							}
						} catch (InterruptedException e) {
						}
					}
				};
				t.start();
				synchronized (lock) {
					lock.wait();
				}
				t.interrupt();
			} catch (InterruptedException e1) {
			}
		}
		this.emptyNotifier.run();
		return workCount == 0;
	}

}
