import tensorflow as tf
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Input, Dense, LSTM, Dropout, Attention, GlobalAveragePooling1D
import numpy as np

class LSTMDemandForecaster:
    def __init__(self, sequence_length, num_features):
        """
        Inicializa el modelo con la forma (shape) de los datos de entrada.
        sequence_length: Número de pasos de tiempo pasados (lags)
        num_features: Cantidad de variables (clima + agro + nlp en total)
        """
        self.sequence_length = sequence_length
        self.num_features = num_features
        self.model = self._build_model()

    def _build_model(self):
        """Construye y compila la arquitectura LSTM + Attention usando Keras Functional API."""
        # 1. Capa de Entrada
        inputs = Input(shape=(self.sequence_length, self.num_features))
        
        # 2. Capa LSTM
        # return_sequences=True es vital para poder usar Mecanismos de Atención en las secuencias
        lstm_out = LSTM(64, return_sequences=True)(inputs)
        lstm_out = Dropout(0.2)(lstm_out)
        
        # 3. Capa de Atención (TensorFlow nativo)
        # Permite al modelo "prestar atención" a días/meses clave de la secuencia de forma ponderada
        attention_out = Attention()([lstm_out, lstm_out])
        
        # 4. Reducir dimensiones post-atención
        # GlobalAveragePooling1D promedia los estados devueltos en la dimensión temporal
        reduced_out = GlobalAveragePooling1D()(attention_out)
        
        # 5. Capa Densa (Salida)
        # Predicción de 1 valor continuo por secuencia (ej. demanda o precio futuro de un cultivo específico)
        outputs = Dense(1, activation='linear')(reduced_out)
        
        # 6. Ensamblaje y compilación
        model = Model(inputs=inputs, outputs=outputs)
        model.compile(optimizer='adam', loss='mse', metrics=['mae'])
        
        return model

    def train(self, X_train, y_train, epochs=50, batch_size=32, validation_data=None):
        """
        Función para entrenar delegadamente en el modelo encapsulado.
        """
        print("Iniciando entrenamiento del modelo LSTM con Atención...")
        history = self.model.fit(
            X_train, y_train,
            epochs=epochs,
            batch_size=batch_size,
            validation_data=validation_data,
            verbose=1
        )
        return history
    
    def predict(self, X_test):
        """Realiza predicciones usando los datos de prueba formateados."""
        return self.model.predict(X_test)
