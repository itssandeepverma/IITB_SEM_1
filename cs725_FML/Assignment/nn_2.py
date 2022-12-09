import numpy as np
import matplotlib.pyplot as plt
import sys
import os
import csv
import numpy as np
import pandas as pd
import math
from sklearn.preprocessing import MinMaxScaler

NUM_FEATS=90
np.random.seed(42)

    
class Net(object):
    def __init__(self, num_layers, num_units):
        
        self.num_layers = num_layers
        self.num_units = num_units
        L = num_layers  
        
        self.para_W = {}
        self.para_b =  {}
    
        
        
        for l in range( L):
            if l==0:
                self.para_W['W' + str(l)]= np.random.uniform(-1, 1, size=( self.num_units , NUM_FEATS))
            else:
                self.para_W['W' + str(l)] = np.random.uniform(-1, 1, size=(self.num_units, self.num_units))
            
            self.para_b['b' + str(l)] = np.random.uniform(-1, 1, size=(self.num_units,1))

            
        self.para_b['b' + str(L)] = np.random.uniform(-1, 1, size=(1, 1))
        self.para_W['W' + str(L)]= np.random.uniform(-1, 1, size=(1, self.num_units))
        
        #print("inside init" , self.para_W)
        
        
    def __call__(self, X):
       
        caches=[]
        A = X
        L =  self.num_layers
        
    
        for l in range(L):
            A_prev = A 
            A, cache = forward_prop(A_prev, self.para_W['W' + str(l)], self.para_b['b' + str(l)], activation = "relu")
            caches.append(cache)
     
        AL, cache = forward_prop(A, self.para_W['W' + str(L)], self.para_b['b' + str(L)], activation = "linear")
        AL=AL.astype(int)
        
        caches.append(cache) 
        
        return AL  , caches
         
        
        
    def backward(self, X, Y, lamda):
    
        AL , caches = self.__call__(X)
        cost =  loss_mse(AL, Y)
    
        grad_W={}
        grad_b={}
        grad_A={}
        
        
        L = self.num_layers
        m = AL.shape[1]
        Y = Y.reshape(AL.shape)
        dAL = ((2.)* ( AL - Y) )/m 
        curr_cache = caches[L]
        grad_A["dA" + str(L-1)], grad_W["dW" + str(L)], grad_b["db" + str(L)] = backward_prop(dAL,lamda , curr_cache, "linear")
    
        for l in range(L-1,-1,-1):
            curr_cache = caches[l]
            dA_prev_t, dW_t, db_t = backward_prop(grad_A["dA" + str(l)], lamda ,  curr_cache, "relu")
            grad_A["dA" + str(l-1)] = dA_prev_t
            grad_W["dW" + str(l )] = dW_t
            grad_b["db" + str(l )] = db_t

        return grad_W , grad_b


    

class Optimizer(object):


    def __init__(self, learning_rate):
        self.learning_rate=learning_rate

    def step(self, weights, biases, delta_weights, delta_biases):
        L = len(weights)
        
        #print("before update" , delta_weights)
        for l in range(L):
            weights["W" + str(l)] = weights["W" + str(l)] - self.learning_rate * delta_weights["dW" + str(l)]
            biases["b" + str(l)] = biases["b" + str(l)] - self.learning_rate * delta_biases["db" + str(l)]
        
        #print("after update" , weights)
        
        return weights,biases
     

def loss_mse(y, y_hat):
    mse=np.square(np.subtract(y,y_hat)).mean()
    return mse

def loss_regularization(weights, biases):
    
    l=len(weights)
    reg_sum=0
    for i in range(l):
        reg_sum=reg_sum + np.sum(np.square( weights["W" + str(i)]))
    
    return reg_sum

def loss_fn(y, y_hat, weights, biases, lamda):
    return loss_mse(y,y_hat).sum()  + lamda*loss_regularization(weights,biases)


def rmse(y, y_hat):
    mse=loss_mse(y,y_hat)
    return math.sqrt(mse)
    
def cross_entropy_loss(y, y_hat):
    m = y.shape[1]
    Y=np.log(y_hat)
    loss = y * Y
    loss= loss.sum()
    loss=(-1/m)*loss  

    return loss


def train(
    net, optimizer, lamda, batch_size, max_epochs,
    train_input, train_target,
    dev_input, dev_target
):
    
    m = train_input.shape[1]
    cost=[]
    cost2=[]
    
    for e in range(max_epochs):
        epoch_loss = 0.
        for i in range(0, m, batch_size):
            batch_input = train_input[:,i:i+batch_size]
            batch_target = train_target[:,i:i+batch_size]
            grad_W,grad_b= net.backward(batch_input, batch_target, lamda)
            net.para_W,net.para_b = optimizer.step(net.para_W,net.para_b, grad_W,grad_b)

        train_pred , cache  = net(train_input)
        train_rmse = loss_fn(train_target, train_pred,net.para_W,net.para_b,lamda)
        cost.append(train_rmse)
        
        dev_pred , cache  = net(dev_input)
        dev_rmse = rmse(dev_target, dev_pred)
        cost2.append(dev_rmse)
        print("epoch :", e)
        print('Regularised MSE on train data: {:.5f}'.format(train_rmse))
        print('RMSE on dev data: {:.5f}'.format(dev_rmse))
        print("")
        
    #plt_graph(cost2,"dev_"+str(batch_size))
    #plt_graph(cost,"train_"+str(batch_size))
    


def get_test_data_predictions(net, inputs):
    A ,c =net(inputs)
    return A


def read_data():

    dev_data=pd.read_csv("regression/data/dev.csv")
    train_data=pd.read_csv("regression/data/train.csv")
    test_data    = pd.read_csv("regression/data/test.csv")
    train_input = train_data.iloc[: , 1:]
    dev_target    = dev_data.iloc[:,:1]
    dev_input   = dev_data.iloc[: , 1:]
    test_input   = test_data.iloc[:,:]
    train_target  = train_data.iloc[:,:1] 
    scaler = MinMaxScaler()
    model = scaler.fit(train_input)
    train_input = model.transform(train_input)
    dev_input=model.transform(dev_input)
    test_input=model.transform(test_input)

    return train_input.T, train_target.T.to_numpy(), dev_input.T, dev_target.T.to_numpy() , test_input.T



def forward_prop(A_prev, W, b, activation):
    if activation == "linear":
        Z = np.dot(W,A_prev) + b
        linear_cache = (A_prev, W, b)
        A=Z
        activation_cache=Z
        
    elif activation == "relu":
        Z=np.dot(W,A_prev) + b
        linear_cache =(A_prev, W, b)
        A=np.maximum(0,Z)
        activation_cache = Z 
        
    cache = (linear_cache, activation_cache)
    return A, cache



def backward_prop(dA, lamda , cache, activation):
    
    linear_cache, activation_cache = cache
    Z=activation_cache
    
    if activation == "relu":
        dZ = np.array(dA, copy=True)  
        dZ[Z <= 0] = 0
        A_prev, W, b = linear_cache
        m = A_prev.shape[1]
        dW = ( np.dot(dZ,A_prev.T) + (lamda/m)*W )*  (1./m)
        db =  ( np.sum(dZ, axis = 1, keepdims = True))* (1./m)
        dA_prev = np.dot(W.T,dZ) 
        
    elif activation == "linear":
        dZ=dA
        A_prev, W, b = linear_cache
        m = A_prev.shape[1]
        dW = ( np.dot(dZ,A_prev.T) + (lamda/m)*W )*  (1./m)
        db =  ( np.sum(dZ, axis = 1, keepdims = True))* (1./m)
        dA_prev = np.dot(W.T,dZ)
    
    return dA_prev, dW, db

def plt_graph(costs, title):
    plt.plot(np.squeeze(costs))
    plt.ylabel('cost')
    plt.title(title)
    plt.xlabel('Epoch')  
    plt.show(title)
    




def main():

    # Hyper-parameters 
    max_epochs = 600
    batch_size = 32
    learning_rate = 0.01
    num_layers = 1
    num_units = 64
    lamda = 0 # Regularization Parameter
    train_input, train_target, dev_input,dev_target, test_input = read_data()
    net = Net(num_layers, num_units)
    optimizer = Optimizer(learning_rate)
    train(
        net, optimizer, lamda, batch_size, max_epochs,
        train_input, train_target,
        dev_input, dev_target
    )
    A=get_test_data_predictions(net, test_input).T
    
    df=pd.DataFrame(data=A,columns=["Predictions"])
    df["Id"]=df.index + 1
    col=["Id","Predictions"]
    df=df.reindex(columns=col)
    df.to_csv('22m0790.csv',index=False)


if __name__ == '__main__':
    main()