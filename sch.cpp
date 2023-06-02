#include <chrono>
#include <functional>
#include <set>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <unordered_map>
#include <iostream>
#include <vector>
#include <algorithm> 

class Task {
public:
    using Clock = std::chrono::system_clock;
    using TimePoint = Clock::time_point;
    using Duration = Clock::duration;

    Task(int id, int priority, std::function<int()> func, TimePoint start_time, TimePoint end_time, Duration interval = Duration::zero())
        : id_(id), priority_(priority), func_(func), start_time_(start_time), end_time_(end_time), interval_(interval), paused_(false), stopped_(false) {} 

    Task()
        : id_(), priority_(), func_(), start_time_(), end_time_(), interval_(), paused_(false), stopped_(false) {}


    int id() const {return id_;}
    int priority() const {return priority_;}

    TimePoint start_time() const {return start_time_;}
    TimePoint end_time() const {return end_time_;}
    Duration interval() const {return interval_;}


    // bool is_paused() const { return paused_; }
    // void pause() { paused_ = true; }
    // void resume() { paused_ = false; }
    bool is_stopped() const { return stopped_; }
    void stop() { stopped_ = true; }

    int execute() const {return func_();}

    // void update_time() {start_time_ = Task::Clock::now() + interval_;}
    void update_time() const {start_time_ = Task::Clock::now() + interval_;}
    bool is_paused() const {return paused_;}
    void pause() const {paused_ = true;}
    void resume() const {paused_ = false;}


private:
    int id_;
    int priority_;
    std::function<int()> func_;
    // TimePoint& start_time_;
    mutable TimePoint start_time_;
    TimePoint& end_time_;
    Duration& interval_;
    // bool paused_;
    mutable bool paused_;
    bool stopped_;
};



class TaskScheduler {

private:
      void run() 
      {
        while (!stop_) 
        {


          const Task& task = get_next_task();

          if (task.is_stopped()) { // 如果任务被停止，则删除任务
              tasks_.erase(tasks_.begin());
              task_map_.erase(task.id());
              continue;
          }

          if(task.is_paused())  // 如果task.is_paused()为1，则输出
          {
            std::cout << "RUN: paused "<< task.id() << ": "<< task.is_paused() << std::endl;
            continue;
          }

          if (task.end_time() <= Task::Clock::now())         // 结束时间 < 当前时间，抛弃此任务
          {
            std::cout << "error! End time is before start time" << std::endl;

            tasks_.erase(tasks_.begin()); 
            task_map_.erase(task.id());   
          }else // 结束时间 >= 当前时间
          {
              if(task.start_time() <= Task::Clock::now())   // 开始时间 <= 当前时间，立即开始
              {
                // 如果任务不是挂起状态
                if (!task.is_paused()) 
                {       

                  if(task.execute() != 0) // 执行成功，删除任务
                  {
                    tasks_.erase(tasks_.begin());
                    task_map_.erase(task.id());                     
                  }
                  else                   // 本次执行失败，删除任务 （以后需要添加：再次执行，三次都执行失败，则删除任务。)
                  {
                    tasks_.erase(tasks_.begin());
                    task_map_.erase(task.id());   
                    printf("Fun run ERROR! Delete Task：%d  \r\n",task.id()); 
                  }
                }
                // 如果任务有时间间隔
                if (task.interval() != Task::Duration::zero()) 
                {
                  task.update_time();
                  add_task(task);    
                }

            }else if(task.start_time() > Task::Clock::now())  // 如果开始时间 > 当前时间，等待时间到达
            {
                std::unique_lock<std::mutex> lock(mutex_);
                std::cv_status status = cond_var_.wait_until(lock, task.start_time());
            }
          }


        }
      }

    // Task get_next_task() {
    //     std::unique_lock<std::mutex> lock(mutex_);
    //     while (tasks_.empty()) {
    //         cond_var_.wait(lock);
    //     }
    //     Task task = *tasks_.begin();
    //     return task;
    // }
    const Task& get_next_task() {
        std::unique_lock<std::mutex> lock(mutex_);
        while (tasks_.empty()) {
            cond_var_.wait(lock);
        }
        const Task& task = *tasks_.begin();
        return task;
    }

    struct TaskComparator {
        bool operator()(const Task& lhs, const Task& rhs) const {
            if (lhs.start_time() == rhs.start_time()) { // 开始时间相同，则按照它们的优先级从高到低排序
                return lhs.priority() > rhs.priority();
            }
            return lhs.start_time() < rhs.start_time(); // 按照开始时间从早到晚排序
        }
    };


    std::multiset<Task, TaskComparator> tasks_;
    std::unordered_map<int, Task> task_map_;
    std::mutex mutex_;
    std::condition_variable cond_var_;
    std::thread worker_thread_;
    std::atomic<bool> stop_;


public:
    TaskScheduler() : stop_(false) {
        worker_thread_ = std::thread([this] { run(); });
    }

    ~TaskScheduler() {
        stop_ = true;
        cond_var_.notify_one();
        worker_thread_.join();
    }

    
    void add_task(const Task& task) {// 添加
        std::unique_lock<std::mutex> lock(mutex_);  
        tasks_.insert(task);                        
        task_map_[task.id()] = task;                
        cond_var_.notify_one();                     
    } 
    
    void cancel_task(int task_id) {// 结束
        std::unique_lock<std::mutex> lock(mutex_);  
        auto it = task_map_.find(task_id);          
        if (it != task_map_.end()) {                
            tasks_.erase(it->second);               
            task_map_.erase(it);                    
        }
    } 
    
    void pause_task(int task_id) {// 挂起
        std::unique_lock<std::mutex> lock(mutex_);
        auto it = task_map_.find(task_id);
        if (it != task_map_.end()) {
            it->second.pause();    
            while (it->second.is_paused()) 
            {
                cond_var_.wait(lock);
            }  
            // std::cout << "VOID pause "<< task_id << ": "<< it->second.is_paused() << std::endl;            
        }
    }
    
    void resume_task(int task_id) {// 恢复
        std::unique_lock<std::mutex> lock(mutex_);
        auto it = task_map_.find(task_id);
        if (it != task_map_.end()) {
            it->second.resume();  
            cond_var_.notify_one();               
        }
    }


    // 1.有开始日期有结束日期（结束时间不小于开始时间）；有开始时间没有结束时间
    void add_calendar_task( int id, int priority, std::function<int()> func, \
      int start_year, int start_month, int start_day, int start_hour = 0, int start_minute = 0, int start_second = 0, \
      int end_year = 0, int end_month = 0, int end_day = 0, int end_hour = 0, int end_minute = 0, int end_second = 0, \
      Task::Duration interval = Task::Duration::zero() ) 
      {

        std::tm start_tm = {};
        start_tm.tm_year = start_year - 1900;
        start_tm.tm_mon = start_month - 1;
        start_tm.tm_mday = start_day;
        start_tm.tm_hour = start_hour;
        start_tm.tm_min = start_minute;
        start_tm.tm_sec = start_second;
        std::time_t start_time_t = std::mktime(&start_tm);
        Task::TimePoint start_time_point = Task::Clock::from_time_t(start_time_t);

        std::tm end_tm = {};
        end_tm.tm_year = end_year - 1900;
        end_tm.tm_mon = end_month - 1;
        end_tm.tm_mday = end_day;
        end_tm.tm_hour = end_hour;
        end_tm.tm_min = end_minute;
        end_tm.tm_sec = end_second;
        std::time_t end_time_t = std::mktime(&end_tm);
        Task::TimePoint end_time_point = Task::Clock::from_time_t(end_time_t);

        // std::cout << "interval: "<< std::chrono::duration_cast<std::chrono::seconds>(interval).count() << " seconds" << std::endl;

        if (end_year != 0 && end_month != 0 && end_day != 0) {
            if (end_time_t < start_time_t) {
                std::cerr << "End time is before start time" << std::endl;
            }else{
               add_task(Task(id, priority, func, start_time_point, end_time_point, interval));
            }
        } else {
            add_task(Task(id, priority, func, start_time_point, Task::Clock::time_point::max(), interval));
        }
    }


    // 2.添加可以在一周中的任何一天执行的重复任务，例如周一、周二、周五上午8:30至下午5:30    这里的12小时制的下午5:30，默认改为24小时制的17:30
    void add_weekly_task(int id, int priority, std::function<int()> func, int start_hour, int start_minute, \
      int end_hour, int end_minute, const std::vector<int>& days, Task::Duration interval = std::chrono::hours(24)) 
      {

        // 获取当前日期和时间
        std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
        std::time_t now_c = std::chrono::system_clock::to_time_t(now);            

        // 将日期和时间转换为struct tm结构体
        std::tm* now_tm = std::localtime(&now_c);

        // 将start_time设置为今天的日期加上指定的时间
        now_tm->tm_hour = start_hour;
        now_tm->tm_min = start_minute;
        now_tm->tm_sec = 0;
        std::time_t start_time_c = std::mktime(now_tm);
        Task::TimePoint start_time = std::chrono::system_clock::from_time_t(start_time_c);

        // 将end_time设置为今天的日期加上指定的时间
        now_tm->tm_hour = end_hour;
        now_tm->tm_min = end_minute;
        now_tm->tm_sec = 0;
        std::time_t end_time_c = std::mktime(now_tm);
        Task::TimePoint end_time = std::chrono::system_clock::from_time_t(end_time_c);

        // 找到下一个应该执行任务的日期
        Task::TimePoint next_time = start_time;
        while (true) {
            std::time_t next_time_t = Task::Clock::to_time_t(next_time);
            std::tm next_tm = *std::localtime(&next_time_t);            
            int next_day = next_tm.tm_wday; // 返回一个整数，表示星期几
            if (std::find(days.begin(), days.end(), next_day) != days.end()) {
                break;
            }
            next_time += std::chrono::hours(24);
        }  

        add_task(Task(id, priority, func, next_time, end_time, interval));
    }


    // 3.给一个开始时间和结束时间添加一个每天都执行的功能（这部分功能，其实可以用add_weekly_task函数周一到周日全部选择）
    void add_daily_task(int id, int priority, std::function<int()> func, int start_hour, int start_minute, \
      int end_hour, int end_minute, Task::Duration interval = std::chrono::hours(24)) 
      {

        std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
        std::time_t now_c = std::chrono::system_clock::to_time_t(now);

        std::tm* now_tm = std::localtime(&now_c);

        now_tm->tm_hour = start_hour;
        now_tm->tm_min = start_minute;
        now_tm->tm_sec = 0;
        std::time_t start_time_c = std::mktime(now_tm);
        Task::TimePoint start_time = std::chrono::system_clock::from_time_t(start_time_c);

        now_tm->tm_hour = end_hour;
        now_tm->tm_min = end_minute;
        now_tm->tm_sec = 0;
        std::time_t end_time_c = std::mktime(now_tm);
        Task::TimePoint end_time = std::chrono::system_clock::from_time_t(end_time_c);

        add_task(Task(id, priority, func, start_time, end_time, interval));
    }

};


int phello(int id) {
    std::cout << "phello from task " << id << std::endl;
    return id;
}


int main() {

    TaskScheduler scheduler;


/*
    // 添加开始时间为2秒，间隔时间3s，结束时间为10秒的任务
    scheduler.add_task(Task(1, 1, []() -> int { return phello(1); }, Task::Clock::now() + std::chrono::seconds(2), Task::Clock::now() + std::chrono::seconds(10), std::chrono::seconds(2)));

    // 从现在起开始时间为1秒，间隔为3秒，不结束
    scheduler.add_task(Task(2, 2, []() -> int { return phello(2); }, Task::Clock::now() + std::chrono::seconds(1), Task::Clock::time_point::max(), std::chrono::seconds(3)));

    // 添加要在特定日期和时间执行的日历任务
    scheduler.add_calendar_task(3, 1, []() -> int { return phello(3); },2023, 05, 12, 16, 35, 00, 2023, 05, 16, 18, 59, 00, std::chrono::seconds(5));

    // 立即执行(开始时间是现在，结束时间是5s后)
    scheduler.add_task(Task(999, 2, []() -> int { return phello(999); }, Task::Clock::now(), Task::Clock::now() + std::chrono::seconds(5)));

    // 每周一、周二和周五从8:30到17:30执行任务，间隔为3s
    std::vector<int> days = {1, 2, 5};
    scheduler.add_weekly_task(4, 4, []() -> int { return phello(4); }, 8, 30, 17, 30, days, std::chrono::seconds(3));

    // 每天都执行的任务，方法一
    // scheduler.add_daily_task(5, 5, []() -> int { return phello(5); }, 8, 30, 17, 30, std::chrono::seconds(4));
    // 每天都执行的任务，方法二
    std::vector<int> days_1 = {1, 2, 3, 4, 5, 6, 7};
    scheduler.add_weekly_task(5, 5, []() -> int { return phello(5); }, 8, 30, 18, 30, days_1, std::chrono::seconds(2));
*/

    // 添加任务888   1s后开始执行，间隔为2秒，不结束
    printf("   \r\n");
    std::cout << "1.ADD " << std::endl;
    scheduler.add_task(Task(888, 2, []() -> int { return phello(888); }, Task::Clock::now() + std::chrono::seconds(1), Task::Clock::time_point::max(), std::chrono::seconds(2)));
    std::this_thread::sleep_for(std::chrono::seconds(4));

    // 暂停任务888  
    printf("   \r\n");
    std::cout << "2.PAUSE " << std::endl;
    scheduler.pause_task(888);
    std::this_thread::sleep_for(std::chrono::seconds(4));

    // 恢复任务888
    printf("   \r\n");
    std::cout << "3.RESUME " << std::endl;
    scheduler.resume_task(888);
    std::this_thread::sleep_for(std::chrono::seconds(4));

    // 删除任务888
    printf("   \r\n");
    std::cout << "4.CANCEL " << std::endl;
    scheduler.cancel_task(888);

    std::this_thread::sleep_for(std::chrono::minutes(1));

    return 0;
}

// sunwin@sunwin-TianYi510Pro-15ICK:~/catkin_ws/src/ROS_DISTRIBUTED/src$ 
// g++ -pthread sch.cpp -o sch
// ./sch 

// 为什么void pause888: 1 执行了，但是接下来的 run paused 888: 0  却读到的为0 ？？？明明已经挂起来了，但是读不到？

