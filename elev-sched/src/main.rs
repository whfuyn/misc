use std::collections::{btree_map, BTreeMap};
use std::future::{poll_fn, Future};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;

use rand::Rng;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{sleep, sleep_until, Duration, Instant};

#[derive(Debug)]
enum Error {
    InvalidRequest,
    ElevatorIsFull,
    ElevatorUnavailable,
    ElevatorNotArrived,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Direction {
    Up,
    Stop,
    Down,
}

struct ElevatorScheduler {
    current_floor: i16,
    moving_direction: Direction,

    passengers: Vec<Rc<Passenger>>,
    stop_at: BTreeMap<i16, Vec<WaitingRequest>>,
    is_stopped: bool,

    reqs_tx: mpsc::Sender<PassengerRequest>,
    reqs_rx: mpsc::Receiver<PassengerRequest>,

    // current_floor, current_direction, is_stopped
    floor_display: watch::Sender<(i16, Direction, bool)>,

    highest_floor: i16,
    lowest_floor: i16,
    capacity: u64,
}

#[derive(Debug)]
struct WaitingRequest {
    at_floor: i16,
    direction: Direction,
}

#[derive(Debug)]
enum PassengerRequestKind {
    Waiting(WaitingRequest),
    Onboard(Rc<Passenger>),
    // Passenger ID
    Offboard(u64),
}

// Default is for taking out resp_tx
#[derive(Debug)]
struct PassengerRequest {
    kind: PassengerRequestKind,
    resp_tx: oneshot::Sender<Result<(), Error>>,
}

impl ElevatorScheduler {
    fn new(highest_floor: i16, lowest_floor: i16, capacity: u64) -> Self {
        let (reqs_tx, reqs_rx) = mpsc::channel(1024);
        let (floor_display, _) = watch::channel((0, Direction::Stop, true));
        Self {
            current_floor: 0,
            moving_direction: Direction::Stop,

            passengers: vec![],
            stop_at: BTreeMap::new(),
            is_stopped: true,

            reqs_tx,
            reqs_rx,

            floor_display,

            highest_floor,
            lowest_floor,
            capacity,
        }
    }

    fn handle_request(&mut self, req: PassengerRequestKind) -> Result<(), Error> {
        // println!("handle request: {req:#?}");
        let floor_range = self.lowest_floor..=self.highest_floor;
        match req {
            PassengerRequestKind::Offboard(id) => {
                if !self.is_stopped {
                    return Err(Error::ElevatorUnavailable);
                }
                let idx = self
                    .passengers
                    .iter()
                    .position(|p| p.id == id)
                    .ok_or(Error::InvalidRequest)?;
                self.passengers.remove(idx);
            }
            PassengerRequestKind::Onboard(passenger) => {
                if !self.is_stopped {
                    return Err(Error::ElevatorUnavailable);
                }
                if passenger.from_floor != self.current_floor {
                    return Err(Error::ElevatorNotArrived);
                }
                if !floor_range.contains(&passenger.to_floor) {
                    return Err(Error::InvalidRequest);
                }
                if self.current_load().saturating_add(passenger.weight as u64) > self.capacity {
                    return Err(Error::ElevatorIsFull);
                }
                self.stop_at
                    .entry(passenger.to_floor)
                    .or_default()
                    .push(WaitingRequest {
                        at_floor: passenger.to_floor,
                        direction: Direction::Stop,
                    });
                self.passengers.push(passenger);
            }
            PassengerRequestKind::Waiting(WaitingRequest {
                at_floor,
                direction,
            }) => {
                if !floor_range.contains(&at_floor)
                    || (at_floor == self.highest_floor && direction == Direction::Up)
                    || (at_floor == self.lowest_floor && direction == Direction::Down)
                {
                    return Err(Error::InvalidRequest);
                }
                self.stop_at
                    .entry(at_floor)
                    .or_default()
                    .push(WaitingRequest {
                        at_floor,
                        direction,
                    });
            }
        };
        Ok(())
    }

    fn get_request_handler_fut<'a>(
        &'a mut self,
        mut exit_cond: impl Future<Output = ()> + Unpin + 'a,
    ) -> impl Future<Output = ()> + 'a {
        poll_fn(move |cx| {
            let mut has_new_reqs = false;
            let exit_cond = &mut exit_cond;
            tokio::pin!(exit_cond);
            loop {
                if exit_cond.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(());
                }

                // Drain all the incoming requests.
                match self.reqs_rx.poll_recv(cx) {
                    Poll::Ready(Some(req)) => {
                        let res = self.handle_request(req.kind);
                        if let Err(e) = &res {
                            match e {
                                Error::InvalidRequest => {
                                    eprintln!("Invalid request");
                                }
                                Error::ElevatorIsFull => {
                                    eprintln!("Elevator is full.");
                                }
                                Error::ElevatorUnavailable => {
                                    eprintln!("Elevator is unavailable.");
                                }
                                Error::ElevatorNotArrived => {
                                    eprintln!("Elevator is not arrived.");
                                }
                            }
                        } else {
                            has_new_reqs = true;
                        }
                        let _ = req.resp_tx.send(res);
                    }
                    Poll::Ready(None) => {
                        // self condition could occur if the sender half of the channel has been dropped,
                        // which might indicate that the scheduler is shutting down.
                        return Poll::Ready(());
                    }
                    Poll::Pending => {
                        // Handling incoming reqs.
                        if self.moving_direction == Direction::Stop && has_new_reqs {
                            return Poll::Ready(());
                        } else {
                            // Otherwise, keep waiting.
                            return Poll::Pending;
                        }
                    }
                }
            }
        })
    }

    fn current_load(&self) -> u64 {
        self.passengers.iter().map(|p| p.weight as u64).sum()
    }

    fn schedule(&mut self) {
        match self.moving_direction {
            Direction::Up => {
                if let Some((&highest_pending, highest_direction)) = self.stop_at.last_key_value() {
                    if self.current_floor == self.highest_floor {
                        self.moving_direction = Direction::Down;
                    }
                    if highest_pending > self.current_floor {
                        self.is_stopped = false;
                    } else if highest_pending < self.current_floor {
                        self.is_stopped = false;
                        self.moving_direction = Direction::Down;
                    } else if highest_pending == self.current_floor {
                        self.is_stopped = true;
                        if highest_direction
                            .iter()
                            .all(|d| d.direction == Direction::Down)
                        {
                            self.moving_direction = Direction::Down;
                        }
                    }
                } else {
                    self.moving_direction = Direction::Stop;
                    self.is_stopped = true;
                }
            }
            Direction::Down => {
                if let Some((&lowest_pending, lowest_direction)) = self.stop_at.first_key_value() {
                    if self.current_floor == self.lowest_floor {
                        self.moving_direction = Direction::Up;
                    }
                    if lowest_pending < self.current_floor {
                        self.is_stopped = false;
                    } else if lowest_pending > self.current_floor {
                        self.is_stopped = false;
                        self.moving_direction = Direction::Up;
                    } else if lowest_pending == self.current_floor {
                        self.is_stopped = true;
                        if lowest_direction
                            .iter()
                            .all(|d| d.direction == Direction::Up)
                        {
                            self.moving_direction = Direction::Up;
                        }
                    }
                } else {
                    self.moving_direction = Direction::Stop;
                    self.is_stopped = true;
                }
            }
            Direction::Stop => {
                let Some((&highest_pending, _)) = self.stop_at.last_key_value() else {
                    self.is_stopped = true;
                    return;
                };
                if highest_pending < self.current_floor {
                    self.is_stopped = false;
                    self.moving_direction = Direction::Down;
                } else if highest_pending > self.current_floor {
                    self.is_stopped = false;
                    self.moving_direction = Direction::Up;
                } else {
                    self.is_stopped = true;
                    self.moving_direction = Direction::Stop;
                }
            }
        }
    }

    fn update_floor_display(&mut self) {
        let info = (self.current_floor, self.moving_direction, self.is_stopped);
        self.floor_display.send_replace(info);
    }

    async fn run(&mut self) {
        loop {
            self.step().await;
        }
    }

    async fn step(&mut self) {
        let moving_time = {
            let t = match self.moving_direction {
                // Waiting for incoming requests.
                Direction::Stop => Duration::MAX,
                // It's a super fast elevator!
                Direction::Up => {
                    self.current_floor += 1;
                    Duration::from_millis(100)
                }
                Direction::Down => {
                    self.current_floor -= 1;
                    Duration::from_millis(100)
                }
            };
            assert!((self.lowest_floor..=self.highest_floor).contains(&self.current_floor));
            sleep(t)
        };
        println!(
            "Direction: {:#?} Floor: {} passengers: {}",
            self.moving_direction,
            self.current_floor,
            self.passengers.len()
        );

        tokio::pin!(moving_time);
        let handling_reqs = self.get_request_handler_fut(moving_time);
        handling_reqs.await;

        // We need to make decision here, otherwise passengers won't know if they should onboard.
        self.schedule();
        if let btree_map::Entry::Occupied(mut stops_ent) = self.stop_at.entry(self.current_floor) {
            let stops = stops_ent.get_mut();
            stops.retain(|req| {
                let needs_stop =
                    // Trying to onboard.
                    req.direction == self.moving_direction
                    // Trying to offboard.
                    || req.direction == Direction::Stop
                    // Currently stopped.
                    || self.moving_direction == Direction::Stop;
                if needs_stop {
                    self.is_stopped = true;
                }
                let completed = !needs_stop;
                completed
            });
            if stops.is_empty() {
                stops_ent.remove();
            }
        }
        self.update_floor_display();

        if self.is_stopped {
            println!("Elevator waiting...");
            let stay_time = sleep(Duration::from_millis(500));
            tokio::pin!(stay_time);
            let handling_reqs = self.get_request_handler_fut(stay_time);

            // Handling requests while staying at the current floor.
            handling_reqs.await;
            println!("Elevator moving...");
        }
        // Schedule again to avoid stuck on the same floor.
        self.schedule();
        self.update_floor_display();
    }
}

#[derive(Debug)]
struct Passenger {
    id: u64,
    weight: u16, // kg

    from_floor: i16,
    to_floor: i16,
    is_onboard: AtomicBool,

    elevator_display: watch::Receiver<(i16, Direction, bool)>,
    elevator_panel: mpsc::Sender<PassengerRequest>,

    arrive_time: Instant,
}

impl Passenger {
    fn new(
        id: u64,
        elevator: &Elevator,
        from_floor: i16,
        to_floor: i16,
        weight: u16,
        arrive_time: Option<Instant>,
    ) -> Self {
        let mut rng = rand::thread_rng();
        let elevator_display = elevator.get_floor_display();
        let elevator_panel = elevator.get_panel();

        let arrive_time: Instant = arrive_time
            .unwrap_or_else(|| Instant::now() + Duration::from_millis(rng.gen_range(0..120000)));
        Passenger {
            id,
            weight,
            from_floor,
            to_floor,
            is_onboard: AtomicBool::new(false),
            elevator_display,
            elevator_panel,
            arrive_time,
        }
    }

    fn new_rc(
        id: u64,
        elevator: &Elevator,
        from_floor: i16,
        to_floor: i16,
        weight: u16,
        arrive_time: Option<Instant>,
    ) -> Rc<Self> {
        let passenger = Self::new(id, elevator, from_floor, to_floor, weight, arrive_time);
        Rc::new(passenger)
    }

    fn generate(
        elevator: &Elevator,
        count: usize,
        highest_floor: i16,
        lowest_floor: i16,
    ) -> Vec<Rc<Self>> {
        let mut rng = rand::thread_rng();
        let mut passengers = Vec::with_capacity(count);
        let now = Instant::now();
        for id in 0..count as u64 {
            let weight = rng.gen_range(30..200);
            let from_floor = rng.gen_range(lowest_floor..highest_floor);
            let to_floor = rng.gen_range(lowest_floor..highest_floor);
            let arrive_time: Instant = now + Duration::from_millis(rng.gen_range(0..10000));

            let passenger = Passenger::new_rc(
                id,
                elevator,
                from_floor,
                to_floor,
                weight,
                Some(arrive_time),
            );
            passengers.push(passenger);
        }
        passengers
    }

    async fn go(self: &Rc<Self>) -> Result<(), Error> {
        let id = self.id;
        sleep_until(self.arrive_time).await;
        // println!("Passenger `{}` start. from {} to {}", id, self.from_floor, self.to_floor);
        if !self.is_onboard.load(Ordering::Relaxed) {
            // println!("Passenger `{}` is calling elevator. from {} to {}", id, self.from_floor, self.to_floor);
            self.call().await?;
            self.wait().await?;
            // println!("elevator arrived at {}", self.from_floor);
            self.onboard().await?;
            println!("Passenger `{}` onboard. from {} to {}", id, self.from_floor, self.to_floor);
        }
        self.wait().await?;
        self.offboard().await?;
        let time_used = Instant::now().duration_since(self.arrive_time);
        println!("Passenger `{}` offboard, time used: `{:#?}`", id, time_used);
        Ok(())
    }

    fn direction_and_floor(&self) -> (Direction, i16) {
        let is_onboard = self.is_onboard.load(Ordering::Relaxed);
        if is_onboard {
            (Direction::Stop, self.to_floor)
        } else if self.from_floor > self.to_floor {
            (Direction::Down, self.from_floor)
        } else if self.from_floor < self.to_floor {
            (Direction::Up, self.from_floor)
        } else {
            (Direction::Stop, self.from_floor)
        }
    }

    async fn send_request(&self, kind: PassengerRequestKind) -> Result<(), Error> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let req = PassengerRequest { kind, resp_tx };
        self.elevator_panel
            .try_send(req)
            .expect("elevator scheduler is down");
        resp_rx.await.or(Err(Error::ElevatorUnavailable))?
    }

    async fn call(&self) -> Result<(), Error> {
        let (direction, at_floor) = self.direction_and_floor();
        let kind = PassengerRequestKind::Waiting(WaitingRequest {
            at_floor,
            direction,
        });

        self.send_request(kind).await
    }

    async fn onboard(self: &Rc<Self>) -> Result<(), Error> {
        let kind = PassengerRequestKind::Onboard(Rc::clone(self));
        let res = self.send_request(kind).await;
        if res.is_ok() {
            // Since it's running on a single thread, a relaxed ordering is OK, right?
            self.is_onboard.store(true, Ordering::Relaxed);
        }
        res
    }

    async fn offboard(&self) -> Result<(), Error> {
        let kind = PassengerRequestKind::Offboard(self.id);
        self.send_request(kind).await
    }

    async fn wait(&self) -> Result<(), Error> {
        let (direction, floor) = self.direction_and_floor();
        self.elevator_display
            .clone()
            .wait_for(|&(elevator_floor, elevator_direction, is_stopped)| {
                elevator_floor == floor
                    && (direction == elevator_direction
                        || direction == Direction::Stop
                        || elevator_direction == Direction::Stop)
                    && is_stopped
            })
            .await
            .map(|_| ())
            .map_err(|_| Error::ElevatorUnavailable)
    }
}

pub struct Elevator {
    scheduler: ElevatorScheduler,
}

impl Elevator {
    pub fn new(highest_floor: i16, lowest_floor: i16, capacity: u64) -> Self {
        let scheduler = ElevatorScheduler::new(highest_floor, lowest_floor, capacity);
        Self { scheduler }
    }

    fn get_floor_display(&self) -> watch::Receiver<(i16, Direction, bool)> {
        self.scheduler.floor_display.subscribe()
    }

    fn get_panel(&self) -> mpsc::Sender<PassengerRequest> {
        self.scheduler.reqs_tx.clone()
    }

    pub async fn start(mut self) {
        self.scheduler.run().await;
    }
}

fn main() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("fail to create runtime");
    let local_set = tokio::task::LocalSet::new();
    let highest_floor = 22;
    let lowest_floor = -2;
    let capacity = 2100;
    // let capacity = u64::MAX;

    let elevator = Elevator::new(highest_floor, lowest_floor, capacity);
    let passengers = Passenger::generate(&elevator, 10, highest_floor, lowest_floor);

    let mut join_handles: Vec<JoinHandle<()>> = vec![];
    passengers.into_iter().for_each(|p| {
        let h = local_set.spawn_local(async move {
            let id = p.id;
            loop {
                let res = p.go().await;
                if let Err(e) = res {
                    eprintln!("Passenger {} failed to arrive destination: {:?}", id, e);
                    eprintln!(
                        "Passenger {} {} -> {} : {:?}",
                        id, p.from_floor, p.to_floor, e
                    );
                    sleep(Duration::from_millis(1000)).await;
                } else {
                    break;
                }
            }
        });
        join_handles.push(h);
    });

    local_set.block_on(&rt, async {
        println!("Starting elevator...");
        let join_all = async move {
            for (i, h) in join_handles.into_iter().enumerate() {
                if let Err(e) = h.await {
                    eprintln!("Passenger {} failed to arrive destination: {:?}", i, e);
                }
            }
        };
        tokio::select! {
            _ = join_all => (),
            _ = elevator.start() => (),
        }
        println!("finished");
    });
}
