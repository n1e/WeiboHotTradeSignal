#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
异常检测模块
实现热搜异常检测和预警功能
"""

import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Tuple
from collections import defaultdict

from logger import logger, log_step, log_error

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


ALERT_TYPE_NEW_TOPIC = 'new_topic'
ALERT_TYPE_RANK_SURGE = 'rank_surge'
ALERT_TYPE_HEAT_SURGE = 'heat_surge'
ALERT_TYPE_SUDDEN_DISAPPEAR = 'sudden_disappear'
ALERT_TYPE_RANK_PLUNGE = 'rank_plunge'

ALERT_LEVEL_NORMAL = 'normal'
ALERT_LEVEL_IMPORTANT = 'important'
ALERT_LEVEL_URGENT = 'urgent'

ALERT_TYPE_NAMES = {
    ALERT_TYPE_NEW_TOPIC: '新上榜话题',
    ALERT_TYPE_RANK_SURGE: '排名暴涨',
    ALERT_TYPE_HEAT_SURGE: '热度爆发',
    ALERT_TYPE_SUDDEN_DISAPPEAR: '突然消失',
    ALERT_TYPE_RANK_PLUNGE: '排名暴跌'
}

ALERT_LEVEL_NAMES = {
    ALERT_LEVEL_NORMAL: '普通',
    ALERT_LEVEL_IMPORTANT: '重要',
    ALERT_LEVEL_URGENT: '紧急'
}


class AnomalyDetector:
    """
    异常检测器"""
    
    def __init__(self, config: Dict[str, Any], storage):
        """
        初始化异常检测器
        
        Args:
            config: 配置字典
            storage: DuckDBStorage 实例
        """
        self.config = config
        self.storage = storage
        self.alert_config = self._load_alert_config()
        self.keywords = self._load_keywords()
        self._last_push_time: Dict[str, datetime] = defaultdict(lambda: datetime(2000, 1, 1))
    
    def _load_alert_config(self) -> Dict[str, Any]:
        """
        加载预警配置
        
        Returns:
            配置字典
        """
        default_config = {
            'enabled': True,
            'new_topic_enabled': True,
            'new_topic_top_rank_threshold': 10,
            'rank_surge_enabled': True,
            'rank_surge_threshold': 30,
            'rank_surge_target_rank': 10,
            'heat_surge_enabled': True,
            'heat_surge_ratio_threshold': 2.0,
            'sudden_disappear_enabled': True,
            'sudden_disappear_days_threshold': 3,
            'rank_plunge_enabled': True,
            'rank_plunge_threshold': 20,
            'rank_plunge_start_rank': 10,
            'alert_level_normal_enabled': True,
            'alert_level_important_enabled': True,
            'alert_level_urgent_enabled': True,
            'normal_push_interval_minutes': 60,
            'important_push_interval_minutes': 30,
            'urgent_push_interval_minutes': 5
        }
        
        try:
            db_config = self.storage.get_alert_config('default')
            if db_config:
                default_config.update(db_config)
                log_step("异常检测", "已从数据库加载预警配置")
        except Exception as e:
            log_error("异常检测", f"加载预警配置失败: {e}", e)
        
        return default_config
    
    def _load_keywords(self) -> List[Dict[str, Any]]:
        """
        加载关注关键词
        
        Returns:
            关键词列表
        """
        try:
            keywords = self.storage.get_alert_keywords()
            log_step("异常检测", f"已加载 {len(keywords)} 个关注关键词")
            return keywords
        except Exception as e:
            log_error("异常检测", f"加载关键词失败: {e}", e)
            return []
    
    def reload_config(self):
        """重新加载配置"""
        self.alert_config = self._load_alert_config()
        self.keywords = self._load_keywords()
    
    def _matches_keyword(self, title: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        检查标题是否匹配关注关键词
        
        Args:
            title: 热搜标题
            
        Returns:
            (是否匹配, 匹配的关键词信息)
        """
        included_keywords = [k for k in self.keywords if k.get('is_included', True)]
        excluded_keywords = [k for k in self.keywords if not k.get('is_included', True)]
        
        for kw in excluded_keywords:
            if kw['keyword'].lower() in title.lower():
                return (False, kw)
        
        if not included_keywords:
            return (True, None)
        
        for kw in included_keywords:
            if kw['keyword'].lower() in title.lower():
                return (True, kw)
        
        return (False, None)
    
    def _get_prev_snapshot(self, current_snapshot_time: datetime) -> Optional[Dict[str, Any]]:
        """
        获取前一个快照数据
        
        Args:
            current_snapshot_time: 当前快照时间
            
        Returns:
            前一个快照数据，如果没有返回None
        """
        try:
            end_time = current_snapshot_time - timedelta(seconds=1)
            start_time = end_time - timedelta(hours=2)
            
            snapshots = self.storage.get_history_by_time_range(
                start_time, end_time, include_items=True
            )
            
            if snapshots:
                return snapshots[0]
            return None
        except Exception as e:
            log_error("异常检测", f"获取前一个快照失败: {e}", e)
            return None
    
    def _build_snapshot_title_map(self, snapshot: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """
        构建快照标题到数据的映射
        
        Args:
            snapshot: 快照数据
            
        Returns:
            标题映射字典
        """
        title_map = {}
        for item in snapshot.get('hot_list', []):
            title = item.get('title', '')
            if title:
                title_map[title] = item
        return title_map
    
    def detect_new_topics(
        self,
        current_snapshot: Dict[str, Any],
        prev_snapshot: Optional[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        检测新上榜话题
        
        Args:
            current_snapshot: 当前快照
            prev_snapshot: 前一个快照
            
        Returns:
            预警事件列表
        """
        if not self.alert_config.get('new_topic_enabled', True):
            return []
        
        alerts = []
        top_rank_threshold = self.alert_config.get('new_topic_top_rank_threshold', 10)
        
        prev_title_map = {}
        if prev_snapshot:
            prev_title_map = self._build_snapshot_title_map(prev_snapshot)
        
        current_time = current_snapshot.get('timestamp', '')
        try:
            current_dt = datetime.fromisoformat(current_time)
        except:
            current_dt = datetime.now()
        
        for item in current_snapshot.get('hot_list', []):
            title = item.get('title', '')
            rank = item.get('rank', 999)
            
            if not title:
                continue
            
            if title not in prev_title_map:
                matches, matched_kw = self._matches_keyword(title)
                if not matches:
                    continue
                
                if rank <= top_rank_threshold:
                    alert_level = ALERT_LEVEL_URGENT if rank <= 3 else ALERT_LEVEL_IMPORTANT
                    
                    details = f"话题「{title}」首次出现在热搜榜第 {rank} 名"
                    if matched_kw:
                        details += f"（匹配关键词: {matched_kw['keyword']}）"
                    
                    alerts.append({
                        'alert_type': ALERT_TYPE_NEW_TOPIC,
                        'alert_level': alert_level,
                        'title': title,
                        'rank_before': None,
                        'rank_after': rank,
                        'rank_change': None,
                        'hot_value_before': None,
                        'hot_value_after': item.get('hot_value'),
                        'heat_change_ratio': None,
                        'snapshot_time_before': prev_snapshot.get('timestamp') if prev_snapshot else None,
                        'snapshot_time_after': current_time,
                        'details': details
                    })
        
        return alerts
    
    def detect_rank_surge(
        self,
        current_snapshot: Dict[str, Any],
        prev_snapshot: Optional[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        检测排名暴涨
        
        Args:
            current_snapshot: 当前快照
            prev_snapshot: 前一个快照
            
        Returns:
            预警事件列表
        """
        if not self.alert_config.get('rank_surge_enabled', True) or not prev_snapshot:
            return []
        
        alerts = []
        surge_threshold = self.alert_config.get('rank_surge_threshold', 30)
        target_rank = self.alert_config.get('rank_surge_target_rank', 10)
        
        prev_title_map = self._build_snapshot_title_map(prev_snapshot)
        current_time = current_snapshot.get('timestamp', '')
        prev_time = prev_snapshot.get('timestamp', '')
        
        for item in current_snapshot.get('hot_list', []):
            title = item.get('title', '')
            current_rank = item.get('rank', 999)
            
            if not title or title not in prev_title_map:
                continue
            
            prev_item = prev_title_map[title]
            prev_rank = prev_item.get('rank', 999)
            
            if prev_rank > current_rank:
                rank_change = prev_rank - current_rank
                
                if rank_change >= surge_threshold and current_rank <= target_rank:
                    matches, matched_kw = self._matches_keyword(title)
                    if not matches:
                        continue
                    
                    if rank_change >= surge_threshold * 2:
                        alert_level = ALERT_LEVEL_URGENT
                    elif rank_change >= surge_threshold * 1.5:
                        alert_level = ALERT_LEVEL_IMPORTANT
                    else:
                        alert_level = ALERT_LEVEL_NORMAL
                    
                    details = f"话题「{title}」排名从 {prev_rank} 名飙升至 {current_rank} 名，上升了 {rank_change} 名"
                    if matched_kw:
                        details += f"（匹配关键词: {matched_kw['keyword']}）"
                    
                    alerts.append({
                        'alert_type': ALERT_TYPE_RANK_SURGE,
                        'alert_level': alert_level,
                        'title': title,
                        'rank_before': prev_rank,
                        'rank_after': current_rank,
                        'rank_change': rank_change,
                        'hot_value_before': prev_item.get('hot_value'),
                        'hot_value_after': item.get('hot_value'),
                        'heat_change_ratio': None,
                        'snapshot_time_before': prev_time,
                        'snapshot_time_after': current_time,
                        'details': details
                    })
        
        return alerts
    
    def detect_heat_surge(
        self,
        current_snapshot: Dict[str, Any],
        prev_snapshot: Optional[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        检测热度爆发
        
        Args:
            current_snapshot: 当前快照
            prev_snapshot: 前一个快照
            
        Returns:
            预警事件列表
        """
        if not self.alert_config.get('heat_surge_enabled', True) or not prev_snapshot:
            return []
        
        alerts = []
        ratio_threshold = self.alert_config.get('heat_surge_ratio_threshold', 2.0)
        
        prev_title_map = self._build_snapshot_title_map(prev_snapshot)
        current_time = current_snapshot.get('timestamp', '')
        prev_time = prev_snapshot.get('timestamp', '')
        
        for item in current_snapshot.get('hot_list', []):
            title = item.get('title', '')
            current_hot = item.get('hot_value', 0)
            
            if not title or title not in prev_title_map:
                continue
            
            prev_item = prev_title_map[title]
            prev_hot = prev_item.get('hot_value', 0)
            
            if prev_hot > 0 and current_hot > 0:
                change_ratio = current_hot / prev_hot
                
                if change_ratio >= ratio_threshold:
                    matches, matched_kw = self._matches_keyword(title)
                    if not matches:
                        continue
                    
                    if change_ratio >= ratio_threshold * 2:
                        alert_level = ALERT_LEVEL_URGENT
                    elif change_ratio >= ratio_threshold * 1.5:
                        alert_level = ALERT_LEVEL_IMPORTANT
                    else:
                        alert_level = ALERT_LEVEL_NORMAL
                    
                    details = f"话题「{title}」热度从 {prev_hot:.0f} 增长至 {current_hot:.0f}，增长了 {(change_ratio-1)*100:.1f}%"
                    if matched_kw:
                        details += f"（匹配关键词: {matched_kw['keyword']}）"
                    
                    alerts.append({
                        'alert_type': ALERT_TYPE_HEAT_SURGE,
                        'alert_level': alert_level,
                        'title': title,
                        'rank_before': prev_item.get('rank'),
                        'rank_after': item.get('rank'),
                        'rank_change': None,
                        'hot_value_before': prev_hot,
                        'hot_value_after': current_hot,
                        'heat_change_ratio': change_ratio,
                        'snapshot_time_before': prev_time,
                        'snapshot_time_after': current_time,
                        'details': details
                    })
        
        return alerts
    
    def detect_sudden_disappear(
        self,
        current_snapshot: Dict[str, Any],
        prev_snapshot: Optional[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        检测突然消失的话题
        
        Args:
            current_snapshot: 当前快照
            prev_snapshot: 前一个快照
            
        Returns:
            预警事件列表
        """
        if not self.alert_config.get('sudden_disappear_enabled', True) or not prev_snapshot:
            return []
        
        alerts = []
        days_threshold = self.alert_config.get('sudden_disappear_days_threshold', 3)
        
        current_title_map = self._build_snapshot_title_map(current_snapshot)
        prev_title_map = self._build_snapshot_title_map(prev_snapshot)
        current_time = current_snapshot.get('timestamp', '')
        prev_time = prev_snapshot.get('timestamp', '')
        
        for title, prev_item in prev_title_map.items():
            if title not in current_title_map:
                prev_rank = prev_item.get('rank', 999)
                
                if prev_rank <= 20:
                    consecutive_days = self.storage.get_consecutive_appear_days(title)
                    
                    if consecutive_days >= days_threshold:
                        matches, matched_kw = self._matches_keyword(title)
                        if not matches:
                            continue
                        
                        if prev_rank <= 10:
                            alert_level = ALERT_LEVEL_URGENT
                        elif prev_rank <= 20:
                            alert_level = ALERT_LEVEL_IMPORTANT
                        else:
                            alert_level = ALERT_LEVEL_NORMAL
                        
                        details = f"话题「{title}」已连续上榜 {consecutive_days} 天，此次突然从热搜榜消失（上次排名第 {prev_rank} 名）"
                        if matched_kw:
                            details += f"（匹配关键词: {matched_kw['keyword']}）"
                        
                        alerts.append({
                            'alert_type': ALERT_TYPE_SUDDEN_DISAPPEAR,
                            'alert_level': alert_level,
                            'title': title,
                            'rank_before': prev_rank,
                            'rank_after': None,
                            'rank_change': None,
                            'hot_value_before': prev_item.get('hot_value'),
                            'hot_value_after': None,
                            'heat_change_ratio': None,
                            'snapshot_time_before': prev_time,
                            'snapshot_time_after': current_time,
                            'details': details
                        })
        
        return alerts
    
    def detect_rank_plunge(
        self,
        current_snapshot: Dict[str, Any],
        prev_snapshot: Optional[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        检测排名暴跌
        
        Args:
            current_snapshot: 当前快照
            prev_snapshot: 前一个快照
            
        Returns:
            预警事件列表
        """
        if not self.alert_config.get('rank_plunge_enabled', True) or not prev_snapshot:
            return []
        
        alerts = []
        plunge_threshold = self.alert_config.get('rank_plunge_threshold', 20)
        start_rank = self.alert_config.get('rank_plunge_start_rank', 10)
        
        prev_title_map = self._build_snapshot_title_map(prev_snapshot)
        current_title_map = self._build_snapshot_title_map(current_snapshot)
        current_time = current_snapshot.get('timestamp', '')
        prev_time = prev_snapshot.get('timestamp', '')
        
        for title, prev_item in prev_title_map.items():
            prev_rank = prev_item.get('rank', 999)
            
            if prev_rank > start_rank:
                continue
            
            if title not in current_title_map:
                current_rank = 51
            else:
                current_rank = current_title_map[title].get('rank', 999)
            
            rank_change = current_rank - prev_rank
            
            if rank_change >= plunge_threshold:
                matches, matched_kw = self._matches_keyword(title)
                if not matches:
                    continue
                
                if rank_change >= plunge_threshold * 2:
                    alert_level = ALERT_LEVEL_URGENT
                elif rank_change >= plunge_threshold * 1.5:
                    alert_level = ALERT_LEVEL_IMPORTANT
                else:
                    alert_level = ALERT_LEVEL_NORMAL
                
                if title not in current_title_map:
                    details = f"话题「{title}」从第 {prev_rank} 名跌出热搜榜"
                else:
                    details = f"话题「{title}」排名从 {prev_rank} 名暴跌至 {current_rank} 名，下跌了 {rank_change} 名"
                
                if matched_kw:
                    details += f"（匹配关键词: {matched_kw['keyword']}）"
                
                alerts.append({
                    'alert_type': ALERT_TYPE_RANK_PLUNGE,
                    'alert_level': alert_level,
                    'title': title,
                    'rank_before': prev_rank,
                    'rank_after': current_rank if title in current_title_map else None,
                    'rank_change': rank_change,
                    'hot_value_before': prev_item.get('hot_value'),
                    'hot_value_after': current_title_map[title].get('hot_value') if title in current_title_map else None,
                    'heat_change_ratio': None,
                    'snapshot_time_before': prev_time,
                    'snapshot_time_after': current_time,
                    'details': details
                })
        
        return alerts
    
    def detect_all(
        self,
        current_snapshot: Dict[str, Any],
        prev_snapshot: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        执行所有异常检测
        
        Args:
            current_snapshot: 当前快照数据
            prev_snapshot: 前一个快照数据（可选，不传则自动获取）
            
        Returns:
            预警事件列表
        """
        if not self.alert_config.get('enabled', True):
            log_step("异常检测", "预警功能已禁用，跳过检测")
            return []
        
        log_step("异常检测", "开始异常检测...")
        
        if prev_snapshot is None:
            try:
                current_time_str = current_snapshot.get('timestamp', '')
                if current_time_str:
                    current_dt = datetime.fromisoformat(current_time_str)
                    prev_snapshot = self._get_prev_snapshot(current_dt)
            except Exception as e:
                log_error("异常检测", f"获取前一个快照失败: {e}", e)
        
        all_alerts = []
        
        new_topic_alerts = self.detect_new_topics(current_snapshot, prev_snapshot)
        all_alerts.extend(new_topic_alerts)
        
        rank_surge_alerts = self.detect_rank_surge(current_snapshot, prev_snapshot)
        all_alerts.extend(rank_surge_alerts)
        
        heat_surge_alerts = self.detect_heat_surge(current_snapshot, prev_snapshot)
        all_alerts.extend(heat_surge_alerts)
        
        sudden_disappear_alerts = self.detect_sudden_disappear(current_snapshot, prev_snapshot)
        all_alerts.extend(sudden_disappear_alerts)
        
        rank_plunge_alerts = self.detect_rank_plunge(current_snapshot, prev_snapshot)
        all_alerts.extend(rank_plunge_alerts)
        
        log_step("异常检测", f"检测完成，共发现 {len(all_alerts)} 个预警事件")
        log_step("异常检测", f"  - 新上榜话题: {len(new_topic_alerts)}")
        log_step("异常检测", f"  - 排名暴涨: {len(rank_surge_alerts)}")
        log_step("异常检测", f"  - 热度爆发: {len(heat_surge_alerts)}")
        log_step("异常检测", f"  - 突然消失: {len(sudden_disappear_alerts)}")
        log_step("异常检测", f"  - 排名暴跌: {len(rank_plunge_alerts)}")
        
        return all_alerts
    
    def save_alerts(self, alerts: List[Dict[str, Any]]) -> List[int]:
        """
        保存预警事件到数据库
        
        Args:
            alerts: 预警事件列表
            
        Returns:
            保存成功的事件ID列表
        """
        saved_ids = []
        
        for alert in alerts:
            event_id = self.storage.save_alert_event(alert)
            if event_id:
                saved_ids.append(event_id)
        
        log_step("异常检测", f"已保存 {len(saved_ids)} 个预警事件到数据库")
        return saved_ids
    
    def should_push(self, alert: Dict[str, Any]) -> bool:
        """
        检查是否应该推送此预警（基于推送间隔）
        
        Args:
            alert: 预警事件
            
        Returns:
            是否应该推送
        """
        alert_level = alert.get('alert_level', ALERT_LEVEL_NORMAL)
        alert_type = alert.get('alert_type', '')
        title = alert.get('title', '')
        
        level_enabled_key = f'alert_level_{alert_level}_enabled'
        if not self.alert_config.get(level_enabled_key, True):
            return False
        
        key = f"{alert_type}_{title}"
        last_push = self._last_push_time.get(key)
        
        if not last_push:
            return True
        
        interval_key = f'{alert_level}_push_interval_minutes'
        interval_minutes = self.alert_config.get(interval_key, 60)
        
        now = datetime.now()
        time_diff = (now - last_push).total_seconds() / 60
        
        return time_diff >= interval_minutes
    
    def mark_pushed(self, alert: Dict[str, Any], event_id: int = None):
        """
        标记预警已推送
        
        Args:
            alert: 预警事件
            event_id: 事件ID（可选）
        """
        alert_type = alert.get('alert_type', '')
        title = alert.get('title', '')
        key = f"{alert_type}_{title}"
        
        self._last_push_time[key] = datetime.now()
        
        if event_id:
            self.storage.mark_alert_pushed(event_id)
    
    def build_alert_message(self, alerts: List[Dict[str, Any]]) -> str:
        """
        构建预警推送消息
        
        Args:
            alerts: 预警事件列表
            
        Returns:
            消息文本
        """
        if not alerts:
            return ""
        
        lines = []
        lines.append("🚨 【热搜异常预警")
        lines.append("=" * 40)
        lines.append(f"检测时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"共检测到 {len(alerts)} 个异常事件")
        lines.append("")
        
        for alert in alerts:
            alert_type = alert.get('alert_type', '')
            alert_level = alert.get('alert_level', '')
            type_name = ALERT_TYPE_NAMES.get(alert_type, alert_type)
            level_name = ALERT_LEVEL_NAMES.get(alert_level, alert_level)
            
            level_icon = {
                ALERT_LEVEL_URGENT: '🔴',
                ALERT_LEVEL_IMPORTANT: '🟡',
                ALERT_LEVEL_NORMAL: '⚪'
            }.get(alert_level, '⚪')
            
            lines.append(f"{level_icon} [{level_name}] {type_name}")
            lines.append(f"   话题: {alert.get('title', '')}")
            
            details = alert.get('details', '')
            if details:
                lines.append(f"   详情: {details}")
            
            rank_before = alert.get('rank_before')
            rank_after = alert.get('rank_after')
            if rank_before and rank_after:
                lines.append(f"   排名变化: {rank_before} -> {rank_after}")
            
            lines.append("")
        
        lines.append("=" * 40)
        lines.append("⚠️ 本预警仅供参考，不构成任何投资建议。")
        
        return "\n".join(lines)


def run_anomaly_detection(
    config: Dict[str, Any],
    storage,
    current_snapshot: Dict[str, Any] = None,
    pusher = None
) -> Dict[str, Any]:
    """
    执行异常检测（独立函数，用于集成到主流程
    
    Args:
        config: 配置字典
        storage: DuckDBStorage 实例
        current_snapshot: 当前快照数据（可选）
        pusher: 推送器实例（可选）
        
    Returns:
        检测结果字典
    """
    result = {
        'success': True,
        'alert_count': 0,
        'push_success': False,
        'alerts': [],
        'saved_ids': []
    }
    
    try:
        detector = AnomalyDetector(config, storage)
        
        if current_snapshot is None:
            current_snapshot = storage.get_latest_snapshot(include_items=True)
        
        if not current_snapshot:
            log_step("异常检测", "没有可用的快照数据，跳过异常检测")
            result['success'] = False
            result['error'] = '没有可用的快照数据'
            return result
        
        alerts = detector.detect_all(current_snapshot)
        result['alerts'] = alerts
        result['alert_count'] = len(alerts)
        
        if alerts:
            saved_ids = detector.save_alerts(alerts)
            result['saved_ids'] = saved_ids
            
            if pusher:
                alerts_to_push = [a for a in alerts if detector.should_push(a)]
                
                if alerts_to_push:
                    message = detector.build_alert_message(alerts_to_push)
                    
                    try:
                        from pusher.manager import get_pusher
                        push_success = pusher.push("热搜异常预警", message)
                        
                        if push_success:
                            result['push_success'] = True
                            for alert in alerts_to_push:
                                detector.mark_pushed(alert)
                            
                            log_step("异常检测", f"已推送 {len(alerts_to_push)} 个预警事件")
                        else:
                            log_error("异常检测", "推送预警消息失败")
                            
                    except Exception as e:
                        log_error("异常检测", f"推送预警消息异常: {e}", e)
                        
        return result
        
    except Exception as e:
        log_error("异常检测", f"异常检测执行失败: {e}", e)
        result['success'] = False
        result['error'] = str(e)
        return result