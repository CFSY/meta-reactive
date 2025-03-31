# Examples

## 1. Install the Reactive Framework

Follow the installation instructions in the main README file.

## 2. Running the examples

### 2.1 `basic` and `temp_monitor`

There are three main files:

- `classic.py`: implementation with the Classic API
- `meta.py`: implementation with the Meta API
- `client.py`: client for streaming

The `classic.py` and `meta.py` files provide the same functionality but with different sets of APIs. Pick one to run.

1. `cd` into the example directory
2. Open two terminal sessions
3. In the first session, run either the classic.py or meta.py file.
    ```bash
    python classic.py
    ```
    ```bash
    python meta.py
    ```
4. Once the sensor values begin appearing, run `client.py` to begin streaming the updates.
    ```bash
    python client.py
    ```
5. Terminate the terminal sessions.

### 2.2 `detector`

1. Run the `detector.py` file to view the detection output.
    ```bash
    python detector.py
    ```