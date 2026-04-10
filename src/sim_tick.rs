#[derive(Clone, Copy, Debug, Default)]
pub struct TickAdvance {
    pub ticks: u32,
    pub tick_years: f32,
}

#[derive(Clone, Debug)]
pub struct StrategicClock {
    years_per_real_second: f32,
    fixed_tick_years: f32,
    max_ticks_per_frame: u32,
    last_wall_time: Option<f64>,
    accumulated_years: f32,
}

impl Default for StrategicClock {
    fn default() -> Self {
        Self {
            years_per_real_second: 0.25,
            fixed_tick_years: 0.02,
            max_ticks_per_frame: 32,
            last_wall_time: None,
            accumulated_years: 0.0,
        }
    }
}

impl StrategicClock {
    pub fn years_per_real_second(&self) -> f32 {
        self.years_per_real_second
    }

    pub fn set_years_per_real_second(&mut self, value: f32) {
        self.years_per_real_second = value.clamp(0.01, 5.0);
    }

    pub fn reset_timebase(&mut self) {
        self.last_wall_time = None;
        self.accumulated_years = 0.0;
    }

    pub fn advance(&mut self, now_seconds: f64, paused: bool) -> TickAdvance {
        let dt_seconds = if let Some(last) = self.last_wall_time {
            (now_seconds - last) as f32
        } else {
            0.0
        };
        self.last_wall_time = Some(now_seconds);

        if paused || !dt_seconds.is_finite() || dt_seconds <= 0.0 {
            return TickAdvance::default();
        }

        let advanced_years = (dt_seconds * self.years_per_real_second).clamp(0.0, 0.5);
        self.accumulated_years += advanced_years;

        let mut ticks = 0;
        while self.accumulated_years >= self.fixed_tick_years && ticks < self.max_ticks_per_frame {
            self.accumulated_years -= self.fixed_tick_years;
            ticks += 1;
        }

        TickAdvance {
            ticks,
            tick_years: self.fixed_tick_years,
        }
    }
}
