export class Queue {
	private concurrency: number;
	private running = 0;
	private queue: Array<() => void> = [];
	private pendingPromises: Set<Promise<unknown>> = new Set();

	constructor(concurrency: number) {
		this.concurrency = concurrency;
	}

	async add<T>(taskOrPromise: (() => Promise<T>) | Promise<T>): Promise<T> {
		// If it's already a promise, wrap it in a function
		const task =
			typeof taskOrPromise === "function" ? taskOrPromise : () => taskOrPromise;

		return new Promise<T>((resolve, reject) => {
			this.queue.push(() => {
				const promise = this.runTask(task, resolve, reject);
				this.pendingPromises.add(promise);
				promise.finally(() => {
					this.pendingPromises.delete(promise);
				});
			});
			this.processQueue();
		});
	}

	async all<T>(
		tasksOrPromises: Array<(() => Promise<T>) | Promise<T>>,
	): Promise<T[]> {
		return Promise.all(
			tasksOrPromises.map((taskOrPromise) => this.add(taskOrPromise)),
		);
	}

	async join(): Promise<void> {
		// Wait for all pending promises to complete
		while (
			this.pendingPromises.size > 0 ||
			this.queue.length > 0 ||
			this.running > 0
		) {
			if (this.pendingPromises.size > 0) {
				await Promise.race(this.pendingPromises);
			} else {
				// Small delay to prevent busy waiting
				await new Promise((resolve) => setTimeout(resolve, 10));
			}
		}
	}

	private async runTask<T>(
		task: () => Promise<T>,
		resolve: (value: T) => void,
		reject: (reason?: unknown) => void,
	): Promise<void> {
		this.running++;
		try {
			const result = await task();
			resolve(result);
		} catch (error) {
			reject(error);
		} finally {
			this.running--;
			this.processQueue();
		}
	}

	private processQueue(): void {
		while (this.running < this.concurrency && this.queue.length > 0) {
			const task = this.queue.shift();
			if (task) {
				task();
			}
		}
	}
}
