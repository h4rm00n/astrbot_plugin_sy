import datetime
import json
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.base import JobLookupError
from astrbot.api import logger
from astrbot.api.event import MessageChain
from astrbot.api.message_components import At, Plain
from .utils import is_outdated, save_reminder_data, HolidayManager, find_compatible_reminder_key
from .reminder_handlers import ReminderMessageHandler, TaskExecutor, ReminderExecutor, SimpleMessageSender

# 使用全局注册表来保存调度器实例
# 现在即使在模块重载后，调度器实例也能保持，我看你还怎么创建新实例（恼）
import sys
if not hasattr(sys, "_GLOBAL_SCHEDULER_REGISTRY"):
    sys._GLOBAL_SCHEDULER_REGISTRY = {
        'scheduler': None
    }
    logger.info("创建全局调度器注册表")
else:
    logger.info("使用现有全局调度器注册表")

class ReminderScheduler:
    def __new__(cls, context, reminder_data, data_file, unique_session=False, config=None):
        # 使用实例属性存储初始化状态
        instance = super(ReminderScheduler, cls).__new__(cls)
        instance._first_init = True  # 首次初始化
        
        logger.info("创建 ReminderScheduler 实例")
        return instance
    
    def __init__(self, context, reminder_data, data_file, unique_session=False, config=None):
        self.context = context
        self.reminder_data = reminder_data
        self.data_file = data_file
        self.unique_session = unique_session
        self.config = config or {}
        
        # 定义微信相关平台列表，用于特殊处理
        self.wechat_platforms = self.config.get("wechat_platforms", ["gewechat", "wechatpadpro", "wecom"])
        
        # 从全局注册表获取调度器，如果不存在则创建
        if sys._GLOBAL_SCHEDULER_REGISTRY['scheduler'] is None:
            sys._GLOBAL_SCHEDULER_REGISTRY['scheduler'] = AsyncIOScheduler()
            logger.info("创建新的全局 AsyncIOScheduler 实例")
        else:
            logger.info("使用现有全局 AsyncIOScheduler 实例")
        
        # 使用全局注册表中的调度器
        self.scheduler = sys._GLOBAL_SCHEDULER_REGISTRY['scheduler']
        
        # 创建节假日管理器
        self.holiday_manager = HolidayManager()
        
        # 如果有现有任务且是重新初始化，清理所有现有任务
        if not getattr(self, '_first_init', True) and self.scheduler.get_jobs():
            logger.info("检测到重新初始化，清理现有任务")
            for job in self.scheduler.get_jobs():
                if job.id.startswith("reminder_"):
                    try:
                        self.scheduler.remove_job(job.id)
                    except JobLookupError:
                        pass
        
        # 初始化任务
        self._init_scheduler()
        
        # 初始化不活跃清理任务
        self._init_inactive_cleanup_job()
        
        # 确保调度器运行
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("启动全局 AsyncIOScheduler")
        
        # 重置首次初始化标志
        self._first_init = False
    
    def _init_scheduler(self):
        '''初始化定时器'''
        logger.info(f"开始初始化调度器，加载 {sum(len(reminders) for reminders in self.reminder_data.values())} 个提醒/任务")
        
        # 清理当前实例关联的所有任务
        for job in self.scheduler.get_jobs():
            if job.id.startswith("reminder_"):
                try:
                    self.scheduler.remove_job(job.id)
                    logger.info(f"移除现有任务: {job.id}")
                except JobLookupError:
                    pass
        
        # 重新添加所有任务
        for group in self.reminder_data:
            for i, reminder in enumerate(self.reminder_data[group]):
                if "datetime" not in reminder:
                    continue
                
                # 处理不完整的时间格式问题
                datetime_str = reminder["datetime"]
                try:
                    if ":" in datetime_str and len(datetime_str.split(":")) == 2 and "-" not in datetime_str:
                        # 处理只有时分格式的时间（如"14:50"）
                        today = datetime.datetime.now()
                        hour, minute = map(int, datetime_str.split(":"))
                        dt = today.replace(hour=hour, minute=minute)
                        if dt < today:  # 如果时间已过，设置为明天
                            dt += datetime.timedelta(days=1)
                        # 更新reminder中的datetime为完整格式
                        reminder["datetime"] = dt.strftime("%Y-%m-%d %H:%M")
                        self.reminder_data[group][i] = reminder
                    dt = datetime.datetime.strptime(reminder["datetime"], "%Y-%m-%d %H:%M")
                except ValueError as e:
                    logger.error(f"无法解析时间格式 '{reminder['datetime']}': {str(e)}，跳过此提醒")
                    continue
                
                # 判断过期
                repeat_type = reminder.get("repeat", "none")
                if (repeat_type == "none" or 
                    not any(repeat_key in repeat_type for repeat_key in ["daily", "weekly", "monthly", "yearly"])) and is_outdated(reminder):
                    logger.info(f"跳过已过期的提醒: {reminder['text']}")
                    continue
                
                # 生成唯一的任务ID，添加时间戳确保唯一性
                timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]  # 包含毫秒
                job_id = f"reminder_{group}_{i}_{timestamp}"
                
                # 确保任务ID唯一性，如果存在则添加额外标识
                while any(job.id == job_id for job in self.scheduler.get_jobs()):
                    import random
                    job_id = f"reminder_{group}_{i}_{timestamp}_{random.randint(1000, 9999)}"
                
                # 根据重复类型设置不同的触发器
                if reminder.get("repeat") == "daily":
                    self.scheduler.add_job(
                        self._reminder_callback,
                        'cron',
                        args=[group, reminder],
                        hour=dt.hour,
                        minute=dt.minute,
                        misfire_grace_time=60,
                        id=job_id
                    )
                    logger.info(f"添加每日提醒: {reminder['text']} 时间: {dt.hour}:{dt.minute} ID: {job_id}")
                elif reminder.get("repeat") == "daily_workday":
                    # 每个工作日重复
                    self.scheduler.add_job(
                        self._check_and_execute_workday,
                        'cron',
                        args=[group, reminder],
                        hour=dt.hour,
                        minute=dt.minute,
                        misfire_grace_time=60,
                        id=job_id
                    )
                    logger.info(f"添加工作日提醒: {reminder['text']} 时间: {dt.hour}:{dt.minute} ID: {job_id}")
                elif reminder.get("repeat") == "daily_holiday":
                    # 每个法定节假日重复
                    self.scheduler.add_job(
                        self._check_and_execute_holiday,
                        'cron',
                        args=[group, reminder],
                        hour=dt.hour,
                        minute=dt.minute,
                        misfire_grace_time=60,
                        id=job_id
                    )
                    logger.info(f"添加节假日提醒: {reminder['text']} 时间: {dt.hour}:{dt.minute} ID: {job_id}")
                elif reminder.get("repeat") == "weekly":
                    self.scheduler.add_job(
                        self._reminder_callback,
                        'cron',
                        args=[group, reminder],
                        day_of_week=dt.weekday(),
                        hour=dt.hour,
                        minute=dt.minute,
                        misfire_grace_time=60,
                        id=job_id
                    )
                    logger.info(f"添加每周提醒: {reminder['text']} 时间: 每周{dt.weekday()+1} {dt.hour}:{dt.minute} ID: {job_id}")
                elif reminder.get("repeat") == "weekly_workday":
                    # 每周的这一天，但仅工作日执行
                    self.scheduler.add_job(
                        self._check_and_execute_workday,
                        'cron',
                        args=[group, reminder],
                        day_of_week=dt.weekday(),  # 保留这个限制，因为"每周"需要指定星期几
                        hour=dt.hour,
                        minute=dt.minute,
                        misfire_grace_time=60,
                        id=job_id
                    )
                    logger.info(f"添加每周工作日提醒: {reminder['text']} 时间: 每周{dt.weekday()+1} {dt.hour}:{dt.minute} ID: {job_id}")
                elif reminder.get("repeat") == "weekly_holiday":
                    # 每周的这一天，但仅法定节假日执行
                    self.scheduler.add_job(
                        self._check_and_execute_holiday,
                        'cron',
                        args=[group, reminder],
                        day_of_week=dt.weekday(),
                        hour=dt.hour,
                        minute=dt.minute,
                        misfire_grace_time=60,
                        id=job_id
                    )
                    logger.info(f"添加每周节假日提醒: {reminder['text']} 时间: 每周{dt.weekday()+1} {dt.hour}:{dt.minute} ID: {job_id}")
                elif reminder.get("repeat") == "monthly":
                    self.scheduler.add_job(
                        self._reminder_callback,
                        'cron',
                        args=[group, reminder],
                        day=dt.day,
                        hour=dt.hour,
                        minute=dt.minute,
                        misfire_grace_time=60,
                        id=job_id
                    )
                    logger.info(f"添加每月提醒: {reminder['text']} 时间: 每月{dt.day}日 {dt.hour}:{dt.minute} ID: {job_id}")
                elif reminder.get("repeat") == "monthly_workday":
                    # 每月的这一天，但仅工作日执行
                    self.scheduler.add_job(
                        self._check_and_execute_workday,
                        'cron',
                        args=[group, reminder],
                        day=dt.day,  # 保留这个限制，因为"每月"需要指定几号
                        hour=dt.hour,
                        minute=dt.minute,
                        misfire_grace_time=60,
                        id=job_id
                    )
                    logger.info(f"添加每月工作日提醒: {reminder['text']} 时间: 每月{dt.day}日 {dt.hour}:{dt.minute} ID: {job_id}")
                elif reminder.get("repeat") == "monthly_holiday":
                    # 每月的这一天，但仅法定节假日执行
                    self.scheduler.add_job(
                        self._check_and_execute_holiday,
                        'cron',
                        args=[group, reminder],
                        day=dt.day,
                        hour=dt.hour,
                        minute=dt.minute,
                        misfire_grace_time=60,
                        id=job_id
                    )
                    logger.info(f"添加每月节假日提醒: {reminder['text']} 时间: 每月{dt.day}日 {dt.hour}:{dt.minute} ID: {job_id}")
                elif reminder.get("repeat") == "yearly":
                    self.scheduler.add_job(
                        self._reminder_callback,
                        'cron',
                        args=[group, reminder],
                        month=dt.month,
                        day=dt.day,
                        hour=dt.hour,
                        minute=dt.minute,
                        misfire_grace_time=60,
                        id=job_id
                    )
                    logger.info(f"添加每年提醒: {reminder['text']} 时间: 每年{dt.month}月{dt.day}日 {dt.hour}:{dt.minute} ID: {job_id}")
                elif reminder.get("repeat") == "yearly_workday":
                    # 每年的这一天，但仅工作日执行
                    self.scheduler.add_job(
                        self._check_and_execute_workday,
                        'cron',
                        args=[group, reminder],
                        month=dt.month,  # 保留这个限制，因为"每年"需要指定月份
                        day=dt.day,      # 保留这个限制，因为"每年"需要指定日期
                        hour=dt.hour,
                        minute=dt.minute,
                        misfire_grace_time=60,
                        id=job_id
                    )
                    logger.info(f"添加每年工作日提醒: {reminder['text']} 时间: 每年{dt.month}月{dt.day}日 {dt.hour}:{dt.minute} ID: {job_id}")
                elif reminder.get("repeat") == "yearly_holiday":
                    # 每年的这一天，但仅法定节假日执行
                    self.scheduler.add_job(
                        self._check_and_execute_holiday,
                        'cron',
                        args=[group, reminder],
                        month=dt.month,
                        day=dt.day,
                        hour=dt.hour,
                        minute=dt.minute,
                        misfire_grace_time=60,
                        id=job_id
                    )
                    logger.info(f"添加每年节假日提醒: {reminder['text']} 时间: 每年{dt.month}月{dt.day}日 {dt.hour}:{dt.minute} ID: {job_id}")
                else:
                    self.scheduler.add_job(
                        self._reminder_callback,
                        'date',
                        args=[group, reminder],
                        run_date=dt,
                        misfire_grace_time=60,
                        id=job_id
                    )
                    logger.info(f"添加一次性提醒: {reminder['text']} 时间: {dt.strftime('%Y-%m-%d %H:%M')} ID: {job_id}")
                
                # 更新提醒数据中的job_id（重载后会生成新的ID）
                # 这样确保数据文件中保存的job_id始终是当前调度器中实际的ID
                reminder["job_id"] = job_id
                self.reminder_data[group][i] = reminder
                
                # 如果有过期时间设置，也添加过期删除任务
                if reminder.get("expire_datetime"):
                    try:
                        expire_dt = datetime.datetime.strptime(reminder["expire_datetime"], "%Y-%m-%d %H:%M")
                        if expire_dt > datetime.datetime.now():
                            expire_job_id = self.add_expire_job(group, reminder, expire_dt, i)
                            if expire_job_id:
                                reminder["expire_job_id"] = expire_job_id
                                logger.info(f"恢复过期删除任务: {reminder['text']} 过期时间: {expire_dt.strftime('%Y-%m-%d %H:%M')}")
                        else:
                            # 过期时间已过，直接删除该提醒
                            logger.info(f"检测到已过期的提醒: {reminder['text']}，将被删除")
                            # 标记需要删除（不在循环中直接删除，避免修改列表问题）
                            reminder["_to_delete"] = True
                    except ValueError as e:
                        logger.error(f"解析过期时间失败: {reminder.get('expire_datetime')}: {str(e)}")
        
        # 清理已标记需要删除的过期提醒
        for group in list(self.reminder_data.keys()):
            self.reminder_data[group] = [
                r for r in self.reminder_data[group] if not r.get("_to_delete", False)
            ]
            # 如果群组没有任何提醒了，删除这个群组的条目
            if not self.reminder_data[group]:
                del self.reminder_data[group]
        
        # 保存更新后的数据到文件（包含新的job_id）
        try:
            from .utils import save_reminder_data
            import asyncio
            
            # 检查是否有正在运行的事件循环
            try:
                loop = asyncio.get_running_loop()
                # 如果有运行中的循环，使用 create_task 异步保存
                asyncio.create_task(save_reminder_data(self.data_file, self.reminder_data))
                logger.info("已提交保存更新后的提醒数据任务（包含新的job_id）")
            except RuntimeError:
                # 没有运行中的循环，直接同步保存
                import json
                with open(self.data_file, "w", encoding='utf-8') as f:
                    json.dump(self.reminder_data, f, ensure_ascii=False)
                logger.info("已同步保存更新后的提醒数据（包含新的job_id）")
        except Exception as e:
            logger.error(f"保存更新后的提醒数据失败: {str(e)}")
    
    async def _check_and_execute_workday(self, unified_msg_origin: str, reminder: dict):
        '''检查当天是否为工作日，如果是则执行提醒'''
        today = datetime.datetime.now()
        logger.info(f"检查日期 {today.strftime('%Y-%m-%d')} 是否为工作日，提醒内容: {reminder['text']}")
        
        is_workday = await self.holiday_manager.is_workday(today)
        logger.info(f"日期 {today.strftime('%Y-%m-%d')} 工作日检查结果: {is_workday}")
        
        if is_workday:
            # 如果是工作日则执行提醒
            logger.info(f"确认今天是工作日，执行提醒: {reminder['text']}")
            await self._reminder_callback(unified_msg_origin, reminder)
        else:
            logger.info(f"今天不是工作日，跳过执行提醒: {reminder['text']}")
    
    async def _check_and_execute_holiday(self, unified_msg_origin: str, reminder: dict):
        '''检查当天是否为法定节假日，如果是则执行提醒'''
        today = datetime.datetime.now()
        logger.info(f"检查日期 {today.strftime('%Y-%m-%d')} 是否为法定节假日，提醒内容: {reminder['text']}")
        
        is_holiday = await self.holiday_manager.is_holiday(today)
        logger.info(f"日期 {today.strftime('%Y-%m-%d')} 法定节假日检查结果: {is_holiday}")
        
        if is_holiday:
            # 如果是法定节假日则执行提醒
            logger.info(f"确认今天是法定节假日，执行提醒: {reminder['text']}")
            await self._reminder_callback(unified_msg_origin, reminder)
        else:
            logger.info(f"今天不是法定节假日，跳过执行提醒: {reminder['text']}")
    
    async def _reminder_callback(self, unified_msg_origin: str, reminder: dict):
        '''提醒回调函数'''
        provider = self.context.get_using_provider()
        
        # 区分提醒和任务
        is_task = reminder.get("is_task", False)
        is_command_task = reminder.get("is_command_task", False)
        
        # 初始化处理器
        task_executor = TaskExecutor(self.context, self.wechat_platforms, self.config)
        reminder_executor = ReminderExecutor(self.context, self.wechat_platforms, self.config)
        simple_sender = SimpleMessageSender(self.context, self.wechat_platforms, self.config)
        
        if provider:
            logger.info(f"使用提供商: {provider.meta().type}")
            if is_command_task:
                # 指令任务模式：直接执行指令，不调用LLM
                await task_executor._execute_command_task(unified_msg_origin, reminder, reminder['text'])
            elif is_task:
                # 普通任务模式：模拟用户发送消息，让AI执行任务
                func_tool = self.context.get_llm_tool_manager()
                logger.info(f"LLM工具管理器加载成功: {func_tool is not None}")
                await task_executor.execute_task(unified_msg_origin, reminder, provider, func_tool)
            else:
                # 提醒模式：只是提醒用户
                await reminder_executor.execute_reminder(unified_msg_origin, reminder, provider)
        else:
            logger.warning(f"没有可用的提供商，使用简单消息")
            await simple_sender.send_simple_message(unified_msg_origin, reminder, is_task, is_command_task)
        
        # 如果是一次性任务（非重复任务），执行后从数据中删除
        if reminder.get("repeat", "none") == "none":
            # 使用兼容性查找来找到正确的数据key
            compatible_key = find_compatible_reminder_key(self.reminder_data, unified_msg_origin)
            if compatible_key:
                # 查找并删除这个提醒
                for i, r in enumerate(self.reminder_data[compatible_key]):
                    if (r.get('text') == reminder.get('text') and 
                        r.get('datetime') == reminder.get('datetime') and
                        r.get('creator_id') == reminder.get('creator_id')):  # 更精确的匹配
                        # 如果有保存的任务ID，尝试删除调度任务
                        if r.get('job_id'):
                            try:
                                self.scheduler.remove_job(r['job_id'])
                                logger.info(f"Removed scheduler job: {r['job_id']}")
                            except Exception as e:
                                logger.warning(f"Failed to remove scheduler job {r['job_id']}: {str(e)}")
                        
                        self.reminder_data[compatible_key].pop(i)
                        logger.info(f"One-time {'task' if is_task else 'reminder'} removed: {reminder['text']}")
                        await save_reminder_data(self.data_file, self.reminder_data)
                        break
    
    def add_job(self, msg_origin, reminder, dt):
        '''添加定时任务'''
        # 生成唯一的任务ID，使用时间戳确保唯一性
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]  # 包含毫秒
        job_id = f"reminder_{msg_origin}_{len(self.reminder_data[msg_origin])-1}_{timestamp}"
        
        # 确保任务ID唯一性，如果存在则添加额外标识
        while any(job.id == job_id for job in self.scheduler.get_jobs()):
            import random
            job_id = f"reminder_{msg_origin}_{len(self.reminder_data[msg_origin])-1}_{timestamp}_{random.randint(1000, 9999)}"
        
        # 根据重复类型设置不同的触发器
        if reminder.get("repeat") == "daily":
            self.scheduler.add_job(
                self._reminder_callback,
                'cron',
                args=[msg_origin, reminder],
                hour=dt.hour,
                minute=dt.minute,
                misfire_grace_time=60,
                id=job_id
            )
        elif reminder.get("repeat") == "daily_workday":
            # 每个工作日重复
            self.scheduler.add_job(
                self._check_and_execute_workday,
                'cron',
                args=[msg_origin, reminder],
                hour=dt.hour,
                minute=dt.minute,
                misfire_grace_time=60,
                id=job_id
            )
        elif reminder.get("repeat") == "daily_holiday":
            # 每个法定节假日重复
            self.scheduler.add_job(
                self._check_and_execute_holiday,
                'cron',
                args=[msg_origin, reminder],
                hour=dt.hour,
                minute=dt.minute,
                misfire_grace_time=60,
                id=job_id
            )
        elif reminder.get("repeat") == "weekly":
            self.scheduler.add_job(
                self._reminder_callback,
                'cron',
                args=[msg_origin, reminder],
                day_of_week=dt.weekday(),
                hour=dt.hour,
                minute=dt.minute,
                misfire_grace_time=60,
                id=job_id
            )
        elif reminder.get("repeat") == "weekly_workday":
            # 每周的这一天，但仅工作日执行
            self.scheduler.add_job(
                self._check_and_execute_workday,
                'cron',
                args=[msg_origin, reminder],
                day_of_week=dt.weekday(),  # 保留这个限制，因为"每周"需要指定星期几
                hour=dt.hour,
                minute=dt.minute,
                misfire_grace_time=60,
                id=job_id
            )
        elif reminder.get("repeat") == "weekly_holiday":
            # 每周的这一天，但仅法定节假日执行
            self.scheduler.add_job(
                self._check_and_execute_holiday,
                'cron',
                args=[msg_origin, reminder],
                day_of_week=dt.weekday(),
                hour=dt.hour,
                minute=dt.minute,
                misfire_grace_time=60,
                id=job_id
            )
        elif reminder.get("repeat") == "monthly":
            self.scheduler.add_job(
                self._reminder_callback,
                'cron',
                args=[msg_origin, reminder],
                day=dt.day,
                hour=dt.hour,
                minute=dt.minute,
                misfire_grace_time=60,
                id=job_id
            )
        elif reminder.get("repeat") == "monthly_workday":
            # 每月的这一天，但仅工作日执行
            self.scheduler.add_job(
                self._check_and_execute_workday,
                'cron',
                args=[msg_origin, reminder],
                day=dt.day,  # 保留这个限制，因为"每月"需要指定几号
                hour=dt.hour,
                minute=dt.minute,
                misfire_grace_time=60,
                id=job_id
            )
        elif reminder.get("repeat") == "monthly_holiday":
            # 每月的这一天，但仅法定节假日执行
            self.scheduler.add_job(
                self._check_and_execute_holiday,
                'cron',
                args=[msg_origin, reminder],
                day=dt.day,
                hour=dt.hour,
                minute=dt.minute,
                misfire_grace_time=60,
                id=job_id
            )
        elif reminder.get("repeat") == "yearly":
            self.scheduler.add_job(
                self._reminder_callback,
                'cron',
                args=[msg_origin, reminder],
                month=dt.month,
                day=dt.day,
                hour=dt.hour,
                minute=dt.minute,
                misfire_grace_time=60,
                id=job_id
            )
        elif reminder.get("repeat") == "yearly_workday":
            # 每年的这一天，但仅工作日执行
            self.scheduler.add_job(
                self._check_and_execute_workday,
                'cron',
                args=[msg_origin, reminder],
                month=dt.month,  # 保留这个限制，因为"每年"需要指定月份
                day=dt.day,      # 保留这个限制，因为"每年"需要指定日期
                hour=dt.hour,
                minute=dt.minute,
                misfire_grace_time=60,
                id=job_id
            )
        elif reminder.get("repeat") == "yearly_holiday":
            # 每年的这一天，但仅法定节假日执行
            self.scheduler.add_job(
                self._check_and_execute_holiday,
                'cron',
                args=[msg_origin, reminder],
                month=dt.month,
                day=dt.day,
                hour=dt.hour,
                minute=dt.minute,
                misfire_grace_time=60,
                id=job_id
            )
        else:
            self.scheduler.add_job(
                self._reminder_callback,
                'date',
                args=[msg_origin, reminder],
                run_date=dt,
                misfire_grace_time=60,
                id=job_id
            )
        logger.info(f"Successfully added job: {job_id}")
        return job_id
    
    def remove_job(self, job_id):
        '''删除定时任务'''
        try:
            self.scheduler.remove_job(job_id)
            logger.info(f"Successfully removed job: {job_id}")
            return True
        except JobLookupError:
            logger.error(f"Job not found: {job_id}")
            return False
    
    def add_expire_job(self, msg_origin, reminder, expire_dt, reminder_index):
        '''添加过期删除任务
        
        Args:
            msg_origin: 会话ID
            reminder: 提醒数据
            expire_dt: 过期时间
            reminder_index: 提醒在列表中的索引
            
        Returns:
            str: 任务ID，失败时返回None
        '''
        try:
            # 生成唯一的过期任务ID
            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]
            expire_job_id = f"expire_{msg_origin}_{reminder_index}_{timestamp}"
            
            # 确保任务ID唯一性
            while any(job.id == expire_job_id for job in self.scheduler.get_jobs()):
                import random
                expire_job_id = f"expire_{msg_origin}_{reminder_index}_{timestamp}_{random.randint(1000, 9999)}"
            
            # 添加过期删除任务
            self.scheduler.add_job(
                self._expire_callback,
                'date',
                args=[msg_origin, reminder],
                run_date=expire_dt,
                misfire_grace_time=60,
                id=expire_job_id
            )
            
            logger.info(f"添加过期删除任务: {expire_job_id}, 过期时间: {expire_dt.strftime('%Y-%m-%d %H:%M')}")
            return expire_job_id
            
        except Exception as e:
            logger.error(f"添加过期删除任务失败: {str(e)}")
            return None
    
    async def _expire_callback(self, unified_msg_origin: str, reminder: dict):
        '''过期删除回调函数'''
        try:
            logger.info(f"执行过期删除: {reminder.get('text', 'unknown')}")
            
            # 使用兼容性查找来找到正确的数据key
            compatible_key = find_compatible_reminder_key(self.reminder_data, unified_msg_origin)
            if not compatible_key:
                logger.warning(f"过期删除: 未找到兼容的key: {unified_msg_origin}")
                return
            
            # 查找并删除这个提醒
            reminders = self.reminder_data.get(compatible_key, [])
            for i, r in enumerate(reminders):
                if (r.get('text') == reminder.get('text') and 
                    r.get('datetime') == reminder.get('datetime') and
                    r.get('creator_id') == reminder.get('creator_id')):
                    
                    # 如果有保存的主任务ID，删除调度任务
                    if r.get('job_id'):
                        try:
                            self.scheduler.remove_job(r['job_id'])
                            logger.info(f"过期删除: 移除主任务 {r['job_id']}")
                        except Exception as e:
                            logger.warning(f"过期删除: 移除主任务失败 {r['job_id']}: {str(e)}")
                    
                    # 从列表中删除
                    self.reminder_data[compatible_key].pop(i)
                    
                    is_task = reminder.get("is_task", False)
                    is_command_task = reminder.get("is_command_task", False)
                    item_type = "指令任务" if is_command_task else ("任务" if is_task else "提醒")
                    
                    logger.info(f"过期删除: {item_type}「{reminder['text']}」已自动删除")
                    await save_reminder_data(self.data_file, self.reminder_data)
                    break
            else:
                logger.warning(f"过期删除: 未找到匹配的提醒: {reminder.get('text', 'unknown')}")
                
        except Exception as e:
            logger.error(f"过期删除回调执行失败: {str(e)}")
    
    # 获取会话ID
    def get_session_id(self, unified_msg_origin, reminder):
        """
        根据会话隔离设置，获取正确的会话ID
        
        Args:
            unified_msg_origin: 原始会话ID
            reminder: 提醒/任务数据
            
        Returns:
            str: 处理后的会话ID
        """
        if not self.unique_session:
            return unified_msg_origin
            
        # 如果启用了会话隔离，并且有创建者ID，则在会话ID中添加用户标识
        creator_id = reminder.get("creator_id")
        if creator_id and ":" in unified_msg_origin:
            # 在群聊环境中添加用户ID
            if (":GroupMessage:" in unified_msg_origin or 
                "@chatroom" in unified_msg_origin or
                ":ChannelMessage:" in unified_msg_origin):
                # 分割会话ID并在末尾添加用户标识
                parts = unified_msg_origin.rsplit(":", 1)
                if len(parts) == 2:
                    return f"{parts[0]}:{parts[1]}_{creator_id}"
        
        return unified_msg_origin
    
    def get_original_session_id(self, session_id):
        """
        从隔离格式的会话ID中提取原始会话ID，用于消息发送
        """
        # 使用新的消息处理器来获取原始会话ID
        message_handler = ReminderMessageHandler(self.context, self.wechat_platforms)
        return message_handler.get_original_session_id(session_id)
    
    # 析构函数不执行操作
    def __del__(self):
        # 不关闭调度器，因为它是全局共享的
        pass

    @staticmethod
    def get_scheduler():
        """获取当前的全局调度器实例"""
        return sys._GLOBAL_SCHEDULER_REGISTRY.get('scheduler')
    
    def _init_inactive_cleanup_job(self):
        '''初始化不活跃清理定时任务'''
        inactive_timeout_hours = self.config.get("inactive_timeout_hours", 0)
        
        if inactive_timeout_hours <= 0:
            logger.info("不活跃自动清理功能已禁用")
            return
        
        # 移除已有的不活跃清理任务
        try:
            self.scheduler.remove_job("inactive_cleanup_job")
        except JobLookupError:
            pass
        
        # 每小时检查一次不活跃用户
        self.scheduler.add_job(
            self._inactive_cleanup_callback,
            'interval',
            hours=1,
            id="inactive_cleanup_job",
            misfire_grace_time=300
        )
        logger.info(f"已添加不活跃清理任务，每小时检查一次，超时时间: {inactive_timeout_hours}小时")
    
    async def _inactive_cleanup_callback(self):
        '''不活跃清理回调函数'''
        try:
            inactive_timeout_hours = self.config.get("inactive_timeout_hours", 0)
            if inactive_timeout_hours <= 0:
                return
            
            logger.info("开始检查不活跃用户提醒...")
            
            current_time = datetime.datetime.now()
            timeout_delta = datetime.timedelta(hours=inactive_timeout_hours)
            
            cleaned_count = 0
            
            # 遍历所有会话
            for session_key in list(self.reminder_data.keys()):
                reminders = self.reminder_data.get(session_key, [])
                
                # 记录需要删除的索引
                to_delete = []
                
                for i, reminder in enumerate(reminders):
                    last_interaction_str = reminder.get("last_interaction")
                    
                    if not last_interaction_str:
                        # 没有互动记录，跳过（可能是旧数据，不清理）
                        continue
                    
                    try:
                        last_interaction = datetime.datetime.strptime(last_interaction_str, "%Y-%m-%d %H:%M")
                        
                        # 检查是否超时
                        if current_time - last_interaction > timeout_delta:
                            to_delete.append(i)
                            logger.info(f"检测到不活跃提醒: {reminder.get('text', 'unknown')}，最后互动: {last_interaction_str}")
                    except ValueError:
                        continue
                
                # 从后往前删除
                for i in sorted(to_delete, reverse=True):
                    reminder = reminders[i]
                    
                    # 删除主任务
                    if reminder.get('job_id'):
                        try:
                            self.scheduler.remove_job(reminder['job_id'])
                        except:
                            pass
                    
                    # 删除过期任务
                    if reminder.get('expire_job_id'):
                        try:
                            self.scheduler.remove_job(reminder['expire_job_id'])
                        except:
                            pass
                    
                    reminders.pop(i)
                    cleaned_count += 1
                
                # 更新数据
                if to_delete:
                    self.reminder_data[session_key] = reminders
                    
                    # 如果会话没有提醒了，删除会话
                    if not reminders:
                        del self.reminder_data[session_key]
            
            if cleaned_count > 0:
                await save_reminder_data(self.data_file, self.reminder_data)
                logger.info(f"不活跃清理完成，共清理 {cleaned_count} 个提醒/任务")
            else:
                logger.debug("不活跃清理检查完成，无需清理")
                
        except Exception as e:
            logger.error(f"不活跃清理失败: {str(e)}") 