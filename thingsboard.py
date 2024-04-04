
class IoMTData(Dataset):
    def __init__(self, mr=default_monitoring_rate_milliseconds,
                 tr=default_test_rate_milliseconds, tw=default_test_window_milliseconds,
                 num_samples=None, subject_ids=[]):

        if not isinstance(mr, int):
            raise(TypeError, "mr (monitoring rate) must be an int: received {}".format(type(mr)))

        if not isinstance(tr, int):
            raise (TypeError, "tr (monitoring rate) must be an int: received {}".format(type(tr)))

        if not isinstance(tw, int):
            raise (TypeError, "tw (monitoring rate) must be an int: received {}".format(type(tw)))

        self.mr, self.tr, self.tw = mr, tr, tw

        self.pool_obj = mp.Pool(mp.cpu_count())

        with open(master_path) as f:
            df = pandas.read_csv(f)
        #data = df.to_dict(orient='records')

        self.meta = []
        for x in df.to_dict(orient='records'):
            if (x['record_id'] not in skip_subjects) and (x['jimignore'] != 1):
                if len(subject_ids) == 0:
                    self.meta = convert_meta(x, self.meta)
                elif "{}".format(x['record_id']).zfill(3) in subject_ids:
                    self.meta = convert_meta(x, self.meta)

        print("{} records loaded".format(len(self.meta)))

        if num_samples is not None:
            n_subjects = len(self.meta)
            self.subject_num = np.random.randint(0, n_subjects, int(num_samples))
            self.end_per = np.random.rand(int(num_samples))
        else:
            self.subject_num = None
            self.end_per = None


        #self.server = SSHTunnelForwarder(
        #    'tb.precise.seas.upenn.edu',
        #    ssh_username=config['credentials']['ssh_username'],
        #    ssh_pkey=os.path.join(Path(__file__).parents[0], ".ssh/id_rsa"),
        #    remote_bind_address=('127.0.0.1', 5432)
        #)
        #
        #self.server.start()
        #print("Connected to PRECISE Thingsboard on: {}:{}".format(self.server.local_bind_address[0],
        #                                                          self.server.local_bind_port))
        #local_bind_port = self.server.local_bind_port # local bind port for ssh tunnel
        local_bind_port = 5432

        params = {
            'database': config['credentials']['database'],
            'user': config['credentials']['username'],
            'password': config['credentials']['password'],
            'host': 'localhost',
            'port': local_bind_port
        }

        self.conn = psycopg2.connect(**params)



    def close(self):
        self.pool_obj.close()
        self.conn.close()
        self.server.stop()

    def __len__(self):
        return len(self.meta)

    def __getitem__(self, idx):
        pass

    def _get_data(self, idx):
        meta_idx = self.meta[idx]

        print(meta_idx)

        out = self._query(meta_idx['first_duid'], meta_idx['first_epoch_start'], meta_idx['first_epoch_end'])
        if meta_idx['first_epoch_end'] == meta_idx['monitoring_epoch_end']:
            return out

        out = out + self._query(meta_idx['second_duid'], meta_idx['second_epoch_start'], meta_idx['second_epoch_end'])
        if meta_idx['second_epoch_end'] == meta_idx['monitoring_epoch_end']:
            return out

        out = out + self._query(meta_idx['third_duid'], meta_idx['third_epoch_start'], meta_idx['third_epoch_end'])
        if meta_idx['third_epoch_end'] == meta_idx['monitoring_epoch_end']:
            return out

        out = out + self._query(meta_idx['fourth_duid'], meta_idx['fourth_epoch_start'], meta_idx['fourth_epoch_end'])
        if meta_idx['fourth_epoch_end'] == meta_idx['monitoring_epoch_end']:
            return out

        raise ValueError("Subject {}: monitoring end doesn't match a watch being taken off".format(meta_idx['subject_id']))

    def _query(self, duid, tstart, tend):
        cursor = self.conn.cursor()
        cursor.execute("SELECT key_id FROM ts_kv_dictionary WHERE key = %s", ('{}_LED_GREEN'.format(duid),))
        res = cursor.fetchone()
        key_id = res[0]
        #print("key id: {}".format(key_id))
        # 5 Hz sampling, 48 hours max = 36 * 60 * 60 * 5 = 648000 < 700000
        cursor.execute("SELECT ts, json_v FROM ts_kv WHERE key = %s AND ts >= %s AND ts <= %s LIMIT 700000", (key_id, tstart, tend))
        res = cursor.fetchall()
        out = [{'epoch': row[0], 'ppg': round(row[1]['val'])} for row in res]
        if len(out) >= 648000:
            warnings.warn("More than 36 hours of data recorded")
            #raise ValueError("Too many entries recorded")
        cursor.close()
        return out


    def extract(self, data, t_start, t_end):
        out = []
        for n, val in enumerate(data.keys()):
            out = out + [
                {'epoch': dv['epoch'],
                 'duid': val,
                 'values': [float(dv['acc']['x']), float(dv['acc']['y']), float(dv['acc']['z'])]}
                for dv in data[val] if t_start <= dv['ts'] <= t_end] #TODO: dv['ts'] or dv['epoch']?
        return out

    def test_epochs(self, data):
        ts = np.array([x['epoch'] for x in data])
        return np.arange(np.min(ts), np.max(ts), self.tr)




    def _get_duid(self, duid_list, tstart, tend):
        print(duid_list)
        for duid in duid_list:
            stmt = self.session.prepare(
                "SELECT * FROM ts_kv_cf WHERE key=? AND ts>=? AND ts<=? LIMIT 1 ALLOW FILTERING").bind(
                ('{}_ACC'.format(duid), tstart, tend))
            q = self.session.execute(stmt)
            if len(q.all()) > 0:
                break
        return duid
