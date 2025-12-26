CREATE TABLE IF NOT EXISTS orders (
                                      order_id INTEGER,
                                      status VARCHAR(20)
);

INSERT INTO orders (order_id, status) VALUES
                                          (101, 'completed'),
                                          (102, 'failed'),
                                          (103, 'completed'),
                                          (104, 'processing'),
                                          (105, 'failed'),
                                          (106, 'completed'),
                                          (107, 'cancelled'),
                                          (108, 'completed'),
                                          (109, 'processing'),
                                          (110, 'failed');