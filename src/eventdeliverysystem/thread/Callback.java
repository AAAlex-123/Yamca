package eventdeliverysystem.thread;

/**
 * Provides a way for a Push- or PullThread to perform an action when it has finished executing.
 * Right before a Push- or PullThread returns, it calls the {@code onCompletion} method of the
 * Callback provided, if it exists.
 *
 * @author Alex Mandelias
 */
@FunctionalInterface
public interface Callback {

	/**
	 * The code to call when the Push- or PullThread finishes executing.
	 *
	 * @param success {@code true} if the Thread terminates successfully, {@code false} otherwise
	 * @param topicName the name of the Topic associated with the Thread
	 * @param cause the Throwable that caused success to be {@code false}. If success is {@code
	 * 		true}, the cause may be {@code null}.
	 */
	void onCompletion(boolean success, String topicName, Throwable cause);
}
