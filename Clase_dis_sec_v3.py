#Importamos los paquetes complementarios:
import pandas as pd

class riegoSecundario:
    """
    Clase Riego Secundario:
    Tiene como objeto principal generar el cuadro de turno para distribución secundaria de una red de riego,
    presentar la cuenta de agua y calcular los caudales para estabilizar el canal.
    -Implementa los procedimientos de cálculo para un esquema de distribución secundaria de riego acordado
    -Calcula volumen y lámina para la cuenta de agua de cada parcela.
    -Calcula los caudales para estabilizar el canal (Simulador)
    """

#    Definición de propiedades de la clase
    f_escala = ((24*60*60)/1000)
    f_lamina = 10

    #Parámetros de configuración de la Inspección
    f_compensa = 0
    vol_riego_p_ha = 0
    #revisar que se calcule a partir de una valor de volumen de la Inspección sobre sup a distribuir
    # vol_riego_p_ha = vol_riego_p / sum(self.padron['sup_emp_reducida'])

#   Constructor de la clase / Propiedades

    def __init__(self,
             padron,
             refuerzo = 0,
             solicitud = 0,
             reservorio = 0,
             caudal_canal = 100,
             dur_turno = 24,
             fecha_inicio = "01-01-2022",
             cabeza_cola = 1,
             modo = 0,
             corte_agua = 0,
             simular = 0,
             volumen_tiempo = 0,
             ):

       self.padron = pd.read_json(padron)  # Objeto json del padrón de riego.
       self.refuerzo = pd.read_json(refuerzo) # Objeto json del los refuerzos vincualdos al padrón.
       self.solicitud = pd.read_json(solicitud) # Objeto json de las solicitudes de riego vinculadas al padrón.
       self.reservorio = pd.read_json(reservorio) # Objeto json de los reservorios vinculados al padrón.
       self.caudal_canal = caudal_canal
       self.dur_turno = dur_turno
       self.fecha_inicio = pd.to_datetime(fecha_inicio, dayfirst = True, errors = 'ignore')
       self.cabeza_cola = pd.read_json(cabeza_cola) # Vector de estrategias de riego de los Cauces.
       self.modo = pd.read_json(modo) #Arreglo de modos de riego Grupos/Subgrupos/Cauces
       self.corte_agua = corte_agua
       self.simular = simular
       self.volumen_tiempo = volumen_tiempo

       '''
       Listado del padrón de regantes: padron
       Duración del turno: dur_turno
       Caudal en cabecera de canal: caudal_canal
       Fecha de inicio de la programación del turno: fecha_inicio
       Estrategia de riego: cabeza_cola
       Tiempo de recorrido de toma: tpo_rec_toma
       Tiempo de recorrido de cauce para riego cabeza - cola: tpo_rec_cabeza_cola
       Tiempo de recorrido de cauce para riego cola - cabeza: tpo_rec_cola_cabeza
       Tiempo de descuelgue: tpo_descuelgue
       Superficie empadronada reducida: sup_emp_reducida
       Superficie adicional: sup_ad
       Superficie restringida: sup_res
       Superficie recibida: sup_rec
       Superficie cedida: sup_cedida
       Hectárea activa: ha_activa
       Hectátrea sí/no :ha_si
       Caudal de refuerzo: caudal_refuerzo
       Duración de refuerzo: dur_refuerzo
       Intensidad de refierzo: int_refuerzo
       Factor de compensación_ f_compensa
       Volumen de riego programado: vol_riego_p
       '''


    def set_segmentar(self):
        '''
        Segmentación de padrón, solicitudes, refuerzos y reservorios en base al modo de riego
        '''
        self.modo
        #Segmentación del padron inicial, solicitudes y refuerzos en subgrupo x cauces
        subpadrones = self.padron.groupby('orden_cauce')
        subsolicitudes = self.solicitud.groupby('orde_cauce')
        subrefuerzos = self.refuerzo.groupby('orden_cauce')

        # Conformación de diccionario de subpadrones x cauces del padron inicial
        subpadron = {}
        for cauce in subpadrones.groups:
            # print (f'Subpadron de Cauce:{group}')
            subpadron[cauce] = subpadrones.get_group(cauce)

        pass




# Métodos generales de la clase: determinación de superficie, volumen, tiempo y caudal.

    def get_sup_riego(self):
        """
        Este método devuelve la superficie de riego que se aplica en el cálculo de turno o para simulación de caudales.
        Es aplicable en los distintos ámbitos: parcela/toma/cauce(hijuela)/canal(inspección)

        Datos:
        1)Superficie empadronada reducida: sup_emp_reducida
        Dato obtenido del servicio (WS) de padrón de DGI.
        Dato principal afectado al cálculo de la superficie de riego.

        2)Superficie adicional:sup_ad
        Superficie adicional que solicita el regante mediante la solicitud de riego.

        3)Superficie restringida: sup_res
        Superficie que compensa el pedio de superficie adicional.
        Lo determina el regante en la solicitud de riego para el balance de riego cuando solicita una sup_ad.

        4)Superficie recibida:sup_rec
        Superficie recibida por pases que son establecidos mediante solicitudes de riego de los regantes.

        5)Superficie cedida:sup_cedida
        Superficie cedida por pases que son establecidos mediante solicitudes de riego de los regantes.

        6)Has activas:ha_activa
        Factor que ajusta la superficie activa para riego.

        7)Condición de ha si/no:ha_si
        Estado de habilitación del padrón obtenido del servicio (SW) de Ha si/no del DGI.

        Resultado:
        Superficie de riego:sup_riego
        Valor obtenido para determinar la superficie válida para riego de un padrón, cauce o canal en un turno determinado.
        En caso de simulación (simular = 1) se devuelve el agregado de las superficies por Grupo/Subgrupo/Cauces-
        """

        self.solicitud["sup_anexa"] = self.solicitud['sup_ad'] - self.solicitud['sup_res']
        self.solicitud["sup_pase"] = self.solicitud['sup_rec'] - self.solicitud['sup_ced']
        self.solicitud["sup_riego"] = (self.padron['sup_emp_reducida'] + self.solicitud["sup_anexa"] + self.solicitud["sup_pase"]) * self.padron["ha_si"] * self.solicitud["ha_activa"]

        if self.simular == 1: #Agrega la superficie por nivel: Grupo, Subgrupo, Cauces. No se tienen en cuenta las solicitudes
           sup_grupo = self.padron.groupby('Grupo').sum()
           sup_subgrupo = self.padron.groupby('Subgrupo').sum()
           sup_cauce = self.padron.groupby('orden_cauce').sum()
           #sup_toma = self.padron.groupby('orden_toma').sum()

           #Composición de un df con los agregados de sup_riego para cada uno de los niveles: grupo/subgrupo/cauce/toma
           sup_riego_df = pd.DataFrame({'Grupo': sup_grupo.sup_emp_reducida,
                                        'Subgrupo':sup_subgrupo.sup_emp_reducida,
                                        'Cauce': sup_cauce.sup_emp_reducida})
                                        #'Toma': sup_toma.sup_emp_reducida})
           return sup_riego_df.fillna(value=0)

        else:
            return self.solicitud["sup_riego"].values

    def get_cap_refuerzo(self):
        """
        Este método devuelve el refuerzo disponible en la inspección que se confirgura con los pozos u otras fuentes de refuerzo.
        considerar que tanto caudal_refuerzo como dur_refuerzo son vectores con los datos obtenidos de la configuración de pozos.

        Datos:
        1)Caudal de refuerzo:caudal_refuerzo
        Dato de caudal parametrizado por el AdT al momento de configurar los pozos en la inspección. Es una lista con los caudales
        de los pozos.
        2)Duración de refuerzo: dur_refuerzo
        Dato de duración que complementa al caudal,parametrizado por el AdT al momento de configurar los pozos en la inspección.
        Es una lista con las duraciones de refuerzo.
        Resultado:
        Capacidad de refuerzo: cap_refuerzo.
        Valor obtenido de las sumas de todos los aportes realizados por los pozos de la inspección que determina el total
        de refuerzo disponible mediante este mecanismo de aporte.
        """
        cap_refuerzo = sum(self.refuerzo['caudal_refuerzo'])*sum(self.refuerzo['dur_refuerzo'])*self.f_escala
        return cap_refuerzo

    def get_reservorio(self):
        """
        Consolida el volumen de los reservorios vinculados al padrón
        """
        reservorio = sum(self.reservorio["volumen"])
        return reservorio

    def get_tpo_recorrido(self):
        '''
        Este método devuelve un vector con el tiempo de recorrido que corresónde a cada parcela del padrón.
        Dependerá de la estrategia de riego cabeza-cola de cada cauce.
        Para determinar el tpo_riego_ha se requiere tomar la suma de todos los valores
        '''

        tpo_recorrido = (self.padron['tpo_rec_toma'] + self.padron['tpo_rec_cabeza_cola'])*self.cabeza_cola['cabeza_cola']
        for i in range(len(tpo_recorrido)):
            if tpo_recorrido[i] == 0:
                tpo_recorrido[i] = self.padron['tpo_rec_cola_cabeza'][i]
            else:
                tpo_recorrido[i] = tpo_recorrido[i]

        return tpo_recorrido

    def get_tpo_red(self):
        '''
        El tiempo de red contempla las particularidades de cada uno de los tiempos de recorrido y descuelgue de cada cauce y toma.
        Este método determina el tpo_red para cada parcela del padron.
        '''
        tpo_red = self.padron['tpo_descuelgue'] - self.get_tpo_recorrido()
        return tpo_red

    def get_tpo_riego_ha(self):
        '''
        Este método devuelve el tiempo de riego por ha que se debe aplicar para la determinación del tiempo de riego del turno.
        Contempla las modalidades de distribución independiente (modo = 0) y secuencial (modo = 1).
        '''
        tpo_riego_ha = (self.dur_turno - sum(self.get_tpo_red())) / sum(self.get_sup_riego())
        return tpo_riego_ha

    def get_tpo_riego(self):
        ''' NOTA: En este método se necesita agregar la lógica que contemple el objeto modo.json
        para determinar el tpo_riego en base al modo de riego'''

        '''
        #Determinación de tiempo para modalidad de riego independiente (modo = 0).
        if self.modo == 0:
            tpo_riego_ha = (self.dur_turno - sum(get_tpo_red())) / sum(self.get_sup_riego())
        #Determianción de tiempo para modalidad de riego secuencial (modo = 1).
        else:
            f_tpo = self.dur_turno / (self.get_vol_riego_ha() * sum(self.get_sup_riego()))
            tpo_riego_ha = f_tpo * self.get_vol_riego_ha()
        '''

        # Si el Modo de riego es SECUENCIAL (modo = 0) el tiempo de riego se determina con:
        if self.modo == 0:
            tpo_riego = [self.get_tpo_riego_ha() * self.get_sup_riego()] + self.get_tpo_red()

        # Si el Modo de riego es INDEPENDIENTE (modo = 1) el tiempo de riego se determina con:
        else:
            tpo_riego = self.dur_turno #REVISAR ESTA ASIGNACION

        return tpo_riego

    def get_vol_riego_ha(self, simular = 0):
        self.simular = simular
        """
        Este método devuelve el volumen de riego por ha que se debe aplicar para la determinación del volumen de riego del turno
        o para la simulación. También se establece por medio del parámetro f_compensa si se contemplarán el aporte de los reservorios.
        La determinación del volumen de riego se plantea en todos los ámbitos:parcela/toma/cauce(hijuela)/canal (inspección)
        El vol_riego será el determinado por el ámbito donde se aplique el método.
        """
        cr = self.caudal_canal / sum(self.get_sup_riego())
        vol_base_ha = cr * self.dur_turno * self.f_escala
        refuerzo = self.get_cap_refuerzo() * self.int_refuerzo
        vol_riego_ha = vol_base_ha * self.f_compensa + ((refuerzo + (self.get_reservorio()) * self.simular) / sum(self.get_sup_riego()))
        return vol_riego_ha

    def get_vol_riego(self, simular = 0):
        volumen = self.get_vol_riego_ha(self.simular) * self.get_sup_riego(simular)
        return volumen

    def set_turno_riego(self):
        '''
        Este método devuelve el dataframe de turnado de toda una inspección, grupo , subgrupo, cauce o toma
        según el alcance de la superficie de riego asignada.
        La tabla se compone de los campos: Caudal, Volumen, Tiempo, Inicio y Fin.
        Este alcance se pasará con la variable lista que se compone de
        la secuancia de superficies organizadas por orden de riego.
        '''
        df_turno = pd.DataFrame({'Caudal': 0.0,
                                 'Volumen': self.get_vol_riego(self.simular),
                                 'Inicio': pd.to_datetime('01-01-2020 00:00'),
                                 'Tiempo': pd.to_timedelta(self.get_tpo_riego(), unit = 'd'),
                                 'Fin': pd.to_datetime('01-01-2020 00:00')
                                 })

        # el tiempo de riego asignado a la parcela se interpreta como timeoffset para la programación de inicio y fin de turno
        df_turno.loc[0,'Inicio'] = self.fecha_inicio
        df_turno.loc[0,'Fin'] = self.fecha_inicio + df_turno.loc[0,'Tiempo']

        # Desarrollo de vector de fechas de inicio y fin a partir de los tiempos de riego aprovechando la operatorias con datos datetime
        n = 1
        while n in range(len(self.padron)):
            df_turno.loc[n,'Fin'] = df_turno.loc[(n - 1),'Fin'] + df_turno.loc[n,'Tiempo']
            df_turno.loc[n,'Inicio'] = df_turno.loc[(n - 1),'Fin']
            n = n + 1

        #Convierto a string los datetime para la presentación en las vistas js
        formato_la ='Fecha:%d-%m-%Y Hora:%H:%M'
        n = 0
        while n in range(len(self.padron)):
            df_turno.loc[n,'Fin'] = df_turno.loc[n,'Fin'].strftime(formato_la)
            df_turno.loc[n,'Inicio'] = df_turno.loc[n,'Inicio'].strftime(formato_la)
            df_turno.loc[n, 'Tiempo'] = str(df_turno.loc[n, 'Tiempo'])
            n = n + 1

        #Determina caudales x cauce para el caso de escenarios de simulación
        if self.simular == 1:
            df_turno['Caudal'] = self.set_simulador()
        #Redeterminación del caudal a partir de los vectores de volumen y tiempo contemplando que Q = V/T
        else:
            df_turno['Caudal'] = (df_turno.Volumen/(self.get_tpo_riego_ha() * self.get_sup_riego()))*(1/self.f_escala)

        #Presenta volumen y tiempo o sólo tiempo de riego en el cuadro de turnos
        if self.volumen_tiempo == 1:
            df_turno['Volumen'] = 0
        else:
            pass

        df_turno['CC'] = self.padron['CC']
        df_turno['PP'] = self.padron['PP']
        df_turno['id_parcela'] = self.padron['id_parcela']

        #Cambia el índice al idPdron y presenta en json con clave en base al índice
        df_turno.index = self.padron['idPadron']
        return df_turno.to_json(orient = 'index', date_format = 'iso', date_unit="s", double_precision = 1)

    def set_cuenta_agua(self):
        '''
                Este método calcula la cuenta de agua en los distintos ámbitos de alcance.
                Se expresa en láminas (mm).
                El volumen programado (vol_riego_p) es suministrado por la inspección como dato.
        '''
        lamina_p = (self.vol_riego_p_ha * self.padron['sup_emp_reducida']).values / self.f_lamina
        volumen = (self.get_vol_riego_ha() * self.get_sup_riego())
        lamina_e = volumen / self.f_lamina

        cuenta_agua = lamina_p - lamina_e
        df_cuenta_agua = pd.DataFrame({'Volumen': volumen,
                                       'Agua_Entregada': lamina_e,
                                       'Balance': cuenta_agua})

        #Compone datos complementarios en el Data Frame antes de generar el json
        df_cuenta_agua['CC'] = self.padron['CC']
        df_cuenta_agua['PP'] = self.padron['PP']
        df_cuenta_agua['id_parcela'] = self.padron['id_parcela']

        df_cuenta_agua.index = self.padron['idPadron']
        return df_cuenta_agua.to_json(orient = 'index', double_precision = 1)

    def set_simulador(self): #Contemplar recibir el padron del escenario de simulación
        caudal = (self.get_vol_riego(simular = 1) / self.get_tpo_riego(simular = 1)) * (1 / self.f_escala)
        return caudal




