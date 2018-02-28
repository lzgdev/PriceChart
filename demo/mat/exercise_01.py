
import numpy as np
import matplotlib.pyplot as plt

X = np.linspace(-np.pi, np.pi, 256,endpoint=True)
C,S = np.cos(X), np.sin(X)

#print("X:", X)
#print("C:", C)
#print("S:", S)
print("S:", str(type(S)))

#plt.plot(C,S)

#plt.plot(X,C)
#plt.plot(X,S)

#plt.show()

