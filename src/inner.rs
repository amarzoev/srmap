/// Just give me a damn terminal logger
// fn logger_pls() -> slog::Logger {
//     use slog::Drain;
//     use slog::Logger;
//     use slog_term::term_full;
//     use std::sync::Mutex;
//     Logger::root(Mutex::new(term_full()).fuse(), o!())
// }

pub mod srmap {
    use std::collections::HashMap;
    use std::hash::Hash;
    use std::sync::{Arc, RwLock};
    use evmap;
    use std::sync::Mutex;
    pub use data::{DataType, Datas, Modification, Operation, Record, Records, TableOperation};


    // Bitmap update functions
    pub fn update_access(bitmap: Vec<usize>, uid: usize, add: bool) -> Vec<usize> {
        let index = uid / 64;
        let offset = uid % 64;
        let bmap_len = bitmap.len();
        let mut updated_map = bitmap;
        if bmap_len <= index {
            // extend the bitmap lazily to accommodate all users.
            if add {
                let num_new_elements = index - (bmap_len - 1);
                for _el in 0..num_new_elements {
                    updated_map.push(0);
                }
            }
            // if this was an access retraction and that portion of the bitmap never existed,
            // then there wasn't ever access to begin with. this will change when we do
            // compression.
        }

        let access = 1 << offset;
        updated_map[index] = updated_map[index] ^ access; // or do i use an or and??? check this

        return updated_map
    }

    pub fn get_access(bitmap: Vec<usize>, uid: usize) -> bool {
        let index = uid / 64;
        let offset = uid % 64;
        let bmap_len = bitmap.len();
        if bmap_len <= index {
            return false
        }

        let mask = 1 << offset;
        let res = bitmap[index] & mask;
        if res == 0 {
            return false
        } else {
            return true
        }
    }

    // SRMap inner structure
    pub struct SRMap<K, V, M>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + std::fmt::Debug + Hash + evmap::ShallowCopy,
        M: Clone,
    {
        pub g_map_r: evmap::ReadHandle<K, V>,
        pub b_map_r: evmap::ReadHandle<(K, V), Vec<usize>>,
        pub global_w: Arc<
            Mutex<(
                evmap::WriteHandle<K, V>,
                evmap::WriteHandle<(K, V), Vec<usize>>,
            )>,
        >,
        pub u_map: Arc<RwLock<HashMap<K, Vec<V>>>>,
        pub id_store: Arc<RwLock<HashMap<usize, usize>>>,
        pub meta: M,
        largest: Arc<RwLock<usize>>,
        g_records: usize,
        // log: slog::Logger,
        initialized: bool,
    }


    impl<K, V, M> Clone for SRMap<K, V, M>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + Hash + std::fmt::Debug + evmap::ShallowCopy,
        M: Clone,
    {
        fn clone(&self) -> Self {
            // let logger = super::logger_pls();
            SRMap {
                g_map_r: self.g_map_r.clone(),
                b_map_r: self.b_map_r.clone(),
                global_w: self.global_w.clone(),
                id_store: self.id_store.clone(),
                largest: self.largest.clone(),
                u_map: self.u_map.clone(),
                meta: self.meta.clone(),
                g_records: self.g_records.clone(),
                initialized: self.initialized.clone(),
                // log: logger,
            }
        }
    }


    impl<K, V, M> SRMap<K, V, M>
    where
        K: Eq + Hash + Clone + std::fmt::Debug,
        V: Clone + Eq + Hash + std::fmt::Debug + evmap::ShallowCopy,
        M: Clone,
    {
        pub fn new(init_m: M) -> SRMap<K, V, M> {
            // let logger = super::logger_pls();
            let (g_map_r, g_map_w) = evmap::new();
            let (b_map_r, b_map_w) = evmap::new();
            SRMap {
                g_map_r: g_map_r,
                global_w: Arc::new(Mutex::new((g_map_w, b_map_w))),
                b_map_r: b_map_r,
                u_map: Arc::new(RwLock::new(HashMap::new())),
                id_store: Arc::new(RwLock::new(HashMap::new())),
                meta: init_m,
                g_records: 0,
                largest: Arc::new(RwLock::new(0 as usize)),
                // log: logger,
                initialized: false,
            }
        }

        pub fn g_map_size(&self) -> usize {
            let mut gm_vec = Vec::new();
            self.g_map_r.for_each(|_, _| gm_vec.push(1));
            gm_vec.len()
        }

        pub fn get_id(&self, uid: usize) -> Option<usize> {
            // println!("id store: {:?}", self.id_store.read().unwrap());
            match self.id_store.read().unwrap().get(&uid) {
                Some(&id) => Some(id.clone()),
                None => None
            }
        }

        // Only the global universe writes to the global map.
        // Writes to user universes will first check to see if the record exists in
        // the global universe. If it does, a bit will be flipped to indicate access.
        // If it doesn't exist in the global universe, the record is added to the user
        // universe.
        pub fn insert(&mut self, k: K, v: Vec<V>, uid: usize) {
            let (ref mut g_map_w, ref mut b_map_w) = *self.global_w.lock().unwrap();
            println!("uid: {}", uid);
            // global map insert.
            if uid == 0 as usize {
                println!("uid is 0 in insert");
                for val in v {
                    self.g_records += 1;

                    // Add (index, value) to global map
                    g_map_w.insert(k.clone(), val.clone());

                    // create new bitmap! no users start off having access except for the global
                    // universe.
                    let mut bit_map = Vec::new();
                    bit_map.push(0);

                    b_map_w.insert((k.clone(), val.clone()), bit_map);
                }
                g_map_w.refresh();
                b_map_w.refresh();

            } else {
                // if value exists in the global map, remove this user's name from restricted access list.
                // otherwise, add record to the user's umap.
                let mut u_map = self.u_map.write().unwrap();
                self.g_map_r.get_and(&k.clone(), |vs| {
                    for val in &v {
                        let mut last_seen = 0;
                        let mut count = 0 as usize;
                        let mut found = false;
                        let mut bmap : Vec<Vec<usize>> = Vec::new();

                        // attempting to find a match for this value in the global map
                        // _that this user does not yet have access to_. if this is successful,
                        // indicate that the value has been found, and update access.
                        // if not successful, insert into umap. repeat for all values.
                        for v in vs {
                            if *v == *val && count >= last_seen && found == false {
                                self.b_map_r.get_and(&(k.clone(), val.clone()), |s| {
                                    // if user doesn't yet have access to a record with a matching
                                    // value in the global map, then update this bitmap to grant
                                    // access. otherwise, add to user map.
                                    match get_access(s[count].clone().to_vec(), uid) {
                                        true => {
                                            last_seen = count;
                                        },
                                        false => {
                                            found = true;
                                            bmap = s.clone().to_vec();
                                        }
                                    }

                                    if !found {
                                        count = count + 1 as usize;
                                    }
                                }
                            );}
                        }

                        if found {
                            // give access
                            // println!("flipping bit");
                            bmap[count] = update_access(bmap[count].clone().to_vec(), uid, true);

                            let bmkey = (k.clone(), val.clone());
                            b_map_w.clear(bmkey.clone());

                            // update the shared bmap
                            for v in &bmap {
                                b_map_w.insert(bmkey.clone(), v.clone());
                            }

                            b_map_w.refresh();

                        } else {
                            // println!("umap insert");
                            // insert into umap
                            let mut add = false;
                            let mut added_vec = None;

                            match u_map.get_mut(&k){
                                Some(vec) => { vec.push(val.clone()); },
                                None => {
                                    let mut new_vec = Vec::new();
                                    new_vec.push(val.clone());
                                    add = true;
                                    added_vec = Some(new_vec);
                                }
                            }

                            if add {
                                u_map.insert(k.clone(), added_vec.unwrap());
                            }
                        }
                    }
                });
            }
        }


        pub fn get(&self, k: &K, uid: usize) -> Option<Vec<V>> {
            let mut u_map = self.u_map.write().unwrap();
            let v_vec = u_map.get_mut(k);

            let mut res_list : Vec<V>;
            if v_vec != None {
                res_list = v_vec.unwrap().clone();
            } else {
                res_list = Vec::new();
            }

            self.g_map_r.get_and(&k, |set| {
                for v in set {
                    let bmap = self
                        .b_map_r
                        .get_and(&(k.clone(), v.clone()), |s| s[0].clone())
                        .unwrap();
                    if get_access(bmap, uid) {
                        res_list.push(v.clone());
                    }
                }
            });

            let mut to_return = Vec::new();
            for x in res_list.iter() {
                to_return.push(x.clone());
            }

            if to_return.len() > 0 {
                return Some(to_return)
            } else {
                return None
            }
        }


        pub fn remove(&mut self, k: &K, uid: usize) {
            let mut u_map = self.u_map.write().unwrap();
            u_map.remove(k);

            self.g_map_r.get_and(&k, |set| {
                for v in set.iter() {
                    let bm_key = &(k.clone(), v.clone());
                    let mut bmap = self.b_map_r.get_and(bm_key, |s| s[0].clone());
                    match bmap {
                        Some(mut bm) => {
                            update_access(bm, uid, false);
                        }
                        None => {}
                    }
                }
            });
        }


        pub fn add_user(&mut self) -> usize {
            // capture new id
            let id = self.largest.read().unwrap().clone();

            // update largest so that next ID is one higher
            let mut largest = self.largest.write().unwrap();
            *largest += 1;

            println!("assigning id: {}", id);

            return id // return internal id

        }


        // Get all records that a given user has access to
        pub fn get_all(&self, uid: usize) -> Option<Vec<(K, V)>> {
            let u_map = self.u_map.write().unwrap();
            let mut to_return : Vec<(K, V)> = Vec::new();

            for (k, v) in u_map.iter() {
                for val in v.iter() {
                    to_return.push((k.clone(), val.clone()));
                }
            }

            let mut buffer = Vec::new();
            self.g_map_r
                .for_each(|k, v| buffer.push((k.clone(), v[0].clone())));

            for (k, val) in buffer.iter() {
                let bmkey = (k.clone(), val.clone());
                let mut bmap = self.b_map_r.get_and(&bmkey, |s| s[0].clone()).unwrap();

                if get_access(bmap, uid) {
                    to_return.push(bmkey);
                }
            }

            if to_return.len() > 0 {
                return Some(to_return)
            } else {
                return None
            }
        }
    }
}
