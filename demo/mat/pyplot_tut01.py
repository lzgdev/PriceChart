
import numpy as np
import matplotlib.pyplot as plt

# evenly sampled time at 222ms intervals
t = np.arange(0.0, 5.0, 0.2)

# red dashes, blue squers and gree triganles
plt.plot(t, t, 'r--', t, t**2, 'bs', t, t**3, 'g^')

plt.show()

