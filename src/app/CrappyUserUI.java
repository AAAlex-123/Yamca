package app;

import java.awt.GridLayout;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.WindowConstants;

import eventDeliverySystem.User;
import eventDeliverySystem.datastructures.Post;

/**
 * Imitates the functioanlity of the android app.
 *
 * @author Alex Mandelias
 */
@SuppressWarnings("serial")
public class CrappyUserUI extends JFrame {

	private final User user;

	// TODO: remove
	public class UserUISub {
		public void notify(String topicName) {
			JOptionPane.showMessageDialog(CrappyUserUI.this,
			        String.format("YOU HAVE A NEW MESSAGE AT '%s'", topicName));
		}
	}

	public CrappyUserUI(boolean existing, String name, String serverIP, int serverPort, Path dir)
	        throws IOException {
		super(name);
		if (existing)
			user = User.loadExisting(serverIP, serverPort, dir, name);
		else
			user = User.createNew(serverIP, serverPort, dir, name);

		user.setUserUISub(new UserUISub());

		setSize(800, 600);
		setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);

		final JPanel main = new JPanel(new GridLayout(2, 2));
		main.add(getPostPanel());
		main.add(getCreateTopicPanel());
		main.add(getPullPanel());
		main.add(getListenForTopicPanel());
		add(main);
	}

	private final JPanel getPostPanel() {
		final JPanel main = new JPanel();
		main.setLayout(new BoxLayout(main, BoxLayout.Y_AXIS));

		final JTextField file_text = new JTextField(20);
		final JTextField topicName = new JTextField(20);
		final JButton    postFile  = new JButton("File");
		final JButton    postText  = new JButton("Text");

		postFile.addActionListener(e -> {
			try {
				final User   user1      = CrappyUserUI.this.user;
				final File   file       = new File(file_text.getText());
				final String posterName = user1.getCurrentProfile().getName();
				final Post   post       = Post.fromFile(file, posterName);
				user1.post(post, topicName.getText());
			} catch (final IOException e1) {
				e1.printStackTrace();
			}
		});

		postText.addActionListener(e -> {
			final User   user1      = CrappyUserUI.this.user;
			final String text       = file_text.getText();
			final String posterName = user1.getCurrentProfile().getName();
			final Post   post       = Post.fromText(text, posterName);
			user1.post(post, topicName.getText());
		});

		main.add(file_text);
		main.add(topicName);
		main.add(postFile);
		main.add(postText);

		return main;
	}

	private final JPanel getCreateTopicPanel() {
		final JPanel main = new JPanel();
		main.setLayout(new BoxLayout(main, BoxLayout.Y_AXIS));

		final JTextField topicName   = new JTextField(20);
		final JButton    createTopic = new JButton("Create");

		createTopic.addActionListener(e -> {
			final User user1 = CrappyUserUI.this.user;
			try {
				user1.createTopic(topicName.getText());
			} catch (final IOException e1) {
				e1.printStackTrace();
			}
		});

		main.add(topicName);
		main.add(createTopic);

		return main;
	}

	private final JPanel getPullPanel() {
		final JPanel main = new JPanel();
		main.setLayout(new BoxLayout(main, BoxLayout.Y_AXIS));

		final JTextField topicName = new JTextField(20);
		final JButton    pull      = new JButton("Pull");

		pull.addActionListener(e -> {
			final User user1 = CrappyUserUI.this.user;
			try {
				user1.pull(topicName.getText());
			} catch (final IOException e1) {
				e1.printStackTrace();
			}
		});

		main.add(topicName);
		main.add(pull);

		return main;
	}

	private final JPanel getListenForTopicPanel() {
		final JPanel main = new JPanel();
		main.setLayout(new BoxLayout(main, BoxLayout.Y_AXIS));

		final JTextField topicName      = new JTextField(20);
		final JButton    lsitenForTopic = new JButton("Listen For Topic");

		lsitenForTopic.addActionListener(e -> {
			final User user1 = CrappyUserUI.this.user;
			try {
				user1.listenForNewTopic(topicName.getText());
			} catch (final IOException e1) {
				e1.printStackTrace();
			}
		});

		main.add(topicName);
		main.add(lsitenForTopic);

		return main;
	}
}
