package app;

import java.awt.*;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.WindowConstants;

import eventDeliverySystem.user.User;
import eventDeliverySystem.user.UserAdapter;
import eventDeliverySystem.user.UserEvent;
import eventDeliverySystem.datastructures.Post;

/**
 * Imitates the functionality of the android app.
 *
 * @author Alex Mandelias
 */
public class CrappyUserUI extends JFrame {

	private final transient User user;

	/**
	 * Creates a Crappy User UI.
	 *
	 * @param existing {@code true} if the profile already exists, {@code false} otherwise
	 * @param name the name of the profile
	 * @param serverIP the IP of the leader broker
	 * @param serverPort the port of the leader broker
	 * @param dir the directory where the profiles are stored
	 *
	 * @throws IOException if an I/O error occurs while creating the User object
	 */
	public CrappyUserUI(boolean existing, String name, String serverIP, int serverPort, Path dir)
	        throws IOException {
		super(name);
		if (existing)
			user = User.loadExisting(serverIP, serverPort, dir, name);
		else
			user = User.createNew(serverIP, serverPort, dir, name);

		user.addUserListener(new UserAdapter() {
			@Override
			public void onMessageReceived(UserEvent e) {
				if (e.success)
					JOptionPane.showMessageDialog(CrappyUserUI.this,
							String.format("YOU HAVE A NEW MESSAGE AT '%s'", e.topicName));
			}
		});

		setSize(800, 600);
		setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);

		JPanel main = createMain();
		addPostPanel(main);
		addOtherPanel(main);
		add(main);

		this.pack();
	}

	private JPanel createMain() {
		return new JPanel(new FlowLayout(FlowLayout.LEFT, 30, 0));
	}

	private void addPostPanel(JPanel origin) {
		final JPanel main = createSection();

		JTextField[] jtfs = new JTextField[2];
		addTextFieldsToJPanel(main, jtfs);

		JButton[] buttons = new JButton[2];
		createButton("File", e -> {
			final User   user1      = CrappyUserUI.this.user;
			final File   file       = new File(jtfs[0].getText());
			final String posterName = user1.getCurrentProfile().getName();
			tryPST(() -> {
				Post post = Post.fromFile(file, posterName);
				user1.post(post, jtfs[1].getText());
			});
		}, buttons, 0);

		createButton("Text", e -> {
			final User   user1      = CrappyUserUI.this.user;
			final String text       = jtfs[0].getText();
			final String posterName = user1.getCurrentProfile().getName();
			final Post   post       = Post.fromText(text, posterName);
			user1.post(post, jtfs[1].getText());
		}, buttons, 1);

		addButtonsToJPanel(main, buttons);

		origin.add(main);
		// return main; // return the newly created JPanel
	}

	private void addOtherPanel(JPanel origin) {
		final JPanel main = createSection();

		JTextField[] jtfs = new JTextField[1];
		addTextFieldsToJPanel(main, jtfs);

		JButton[] buttons = new JButton[5];
		createButton("Create", e -> tryPST(() -> user.createTopic(jtfs[0].getText()))
				, buttons, 0);
		createButton("Delete", e -> tryPST(() -> user.deleteTopic(jtfs[0].getText()))
				, buttons, 1);
		createButton("Pull", e -> tryPST(() -> user.pull(jtfs[0].getText()))
				, buttons, 2);
		createButton("Listen", e -> tryPST(() -> user.listenForNewTopic(jtfs[0].getText()))
				, buttons, 3);
		createButton("Stop Listen", e -> tryPST(() -> user.stopListeningForTopic(jtfs[0].getText()))
				, buttons, 4);

		addButtonsToJPanel(main, buttons);

		origin.add(main);
		// return main; // return the newly created JPanel
	}

	private JPanel createSection() {
		return new JPanel(new BorderLayout(15, 15));
	}

	private void addTextFieldsToJPanel(JPanel origin, JTextField[] jtfs) {

		JPanel main = new JPanel(new GridLayout(jtfs.length, 1, 0, 10));

		for (int i = 0; i < jtfs.length; i++) {
			jtfs[i] = new JTextField(15);
			main.add(jtfs[i]);
		}

		origin.add(main, BorderLayout.CENTER);
		// return main; // return the newly created JPanel
	}

	private void addButtonsToJPanel(JPanel origin, JButton... buttons) {
		JPanel main = new JPanel(new FlowLayout(FlowLayout.CENTER, 15, 0));
		for (final JButton button : buttons) {
			main.add(button);
		}

		origin.add(main, BorderLayout.SOUTH);
		// return main; // return the newly created JPanel
	}

	private void createButton(String text, ActionListener l, JButton[] array, int index) {
		JButton button = new JButton(text);
		button.addActionListener(l);
		array[index] = button;
	}

	private interface ExceptionRunnable { void run() throws IOException; }

	private void tryPST(ExceptionRunnable r) {
		try {
			r.run();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
