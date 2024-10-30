from kafka import KafkaConsumer
import json
import traceback
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from collections import deque
import threading
import time

# Initialize data storage
max_length = 100  # Number of data points to display
eeg_data = {
    'Fp1': deque(maxlen=max_length),
    'Fp2': deque(maxlen=max_length),
    'F3': deque(maxlen=max_length),
    'F4': deque(maxlen=max_length),
    'C3': deque(maxlen=max_length),
    'C4': deque(maxlen=max_length),
    'P3': deque(maxlen=max_length),
    'P4': deque(maxlen=max_length),
    'timestamp': deque(maxlen=max_length)
}

# Lock for thread-safe operations on eeg_data
data_lock = threading.Lock()

def kafka_consumer_thread():
    """
    Function to run Kafka consumer in a separate thread.
    Consumes messages and updates the eeg_data deque.
    """
    try:
        consumer = KafkaConsumer(
            'brain-eeg-data',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print("Kafka consumer initialized successfully.")
    except Exception as e:
        print(f"Failed to initialize Kafka consumer: {e}")
        traceback.print_exc()
        return

    try:
        for message in consumer:
            data = message.value
            print(f"Received data: {data}")
            # Update the eeg_data deque in a thread-safe manner
            with data_lock:
                eeg_data['timestamp'].append(data.get('timestamp', int(time.time())))
                channels = data.get('channels', {})
                for channel in ['Fp1', 'Fp2', 'F3', 'F4', 'C3', 'C4', 'P3', 'P4']:
                    eeg_value = channels.get(channel, 0)  # Default to 0 if channel data is missing
                    eeg_data[channel].append(eeg_value)
    except KeyboardInterrupt:
        print("Consumer stopped by user.")
    except Exception as e:
        print(f"Unexpected error in consumer thread: {e}")
        traceback.print_exc()
    finally:
        consumer.close()
        print("Kafka consumer closed.")

def initialize_plot():
    """
    Initializes the matplotlib figure and subplots for each EEG channel.
    Returns the figure, axes, and line objects.
    """
    import seaborn as sns
    sns.set_theme(style="darkgrid")

    fig, axs = plt.subplots(4, 2, figsize=(15, 10))
    fig.suptitle('Real-Time EEG Data Stream', fontsize=16)

    channel_names = ['Fp1', 'Fp2', 'F3', 'F4', 'C3', 'C4', 'P3', 'P4']
    lines = {}

    for ax, channel in zip(axs.flatten(), channel_names):
        line, = ax.plot([], [], label=channel)
        ax.set_xlim(0, max_length)
        ax.set_ylim(-100, 100)  # Adjust based on expected EEG voltage ranges
        ax.set_title(channel)
        ax.legend(loc='upper right')
        lines[channel] = line

    return fig, axs, lines, channel_names

def animate(i, lines, channel_names):
    """
    Animation function called periodically to update the plots.
    """
    with data_lock:
        # Get the current data from eeg_data
        data_points = len(eeg_data['timestamp'])
        for channel in channel_names:
            y_data = list(eeg_data[channel])
            x_data = list(range(data_points))  # Simple x-axis as sequence numbers
            lines[channel].set_data(x_data, y_data)

    return lines.values()

def main():
    # Start the Kafka consumer in a separate thread
    consumer_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    consumer_thread.start()

    # Initialize the plot
    fig, axs, lines, channel_names = initialize_plot()

    # Start the animation
    ani = animation.FuncAnimation(
        fig,
        animate,
        fargs=(lines, channel_names),
        init_func=lambda: [line.set_data([], []) for line in lines.values()],
        interval=100,  # Update every 100ms
        blit=False    # Set blit to False to prevent '_resize_id' errors
    )

    print("Starting EEG data visualization. Close the plot window to stop.")

    try:
        plt.show()
    except KeyboardInterrupt:
        print("Visualization stopped by user.")
    except Exception as e:
        print(f"Unexpected error during visualization: {e}")
        traceback.print_exc()
    finally:
        print("Shutting down...")

if __name__ == "__main__":
    main()
