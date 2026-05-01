#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
微博热搜交易信号分析器 - 主程序
"""

import json
import os
import sys
import argparse
from datetime import datetime
from typing import Optional, Dict, Any

from weibo_scraper import WeiboScraper
from ai_analyzer import AIAnalyzer
from report_generator import ReportGenerator
from logger import setup_logger, get_logger, log_step, log_error, log_push_result
from scheduler import TaskScheduler, run_with_scheduler
from pusher.manager import PushManager, get_push_manager, reset_push_manager


logger = None


def load_config(config_path='config.json'):
    """加载配置文件"""
    if not os.path.exists(config_path):
        print(f"配置文件不存在: {config_path}")
        print(f"请复制 config.example.json 为 config.json 并填写配置信息")
        return None
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config
    except json.JSONDecodeError as e:
        print(f"配置文件格式错误: {e}")
        return None
    except Exception as e:
        print(f"加载配置文件失败: {e}")
        return None


def check_config(config):
    """检查配置是否完整"""
    errors = []
    
    weibo_config = config.get('weibo', {})
    if not weibo_config.get('cookie_sub'):
        errors.append("缺少微博cookie_sub配置")
    
    openrouter_config = config.get('openrouter', {})
    if not openrouter_config.get('api_key'):
        errors.append("缺少OpenRouter API密钥配置")
    
    if errors:
        print("配置检查失败:")
        for error in errors:
            print(f"  - {error}")
        return False
    
    return True


def init_logging(config):
    """初始化日志系统"""
    global logger
    logging_config = config.get('logging', {})
    setup_logger(logging_config)
    logger = get_logger()


def run_once(config, args):
    """
    执行一次完整的分析流程
    
    Args:
        config: 配置
        args: 命令行参数
        
    Returns:
        执行结果字典
    """
    result = {
        'success': False,
        'html_path': None,
        'json_path': None,
        'analysis_result': None,
        'current_data': None,
        'push_results': {}
    }
    
    try:
        logger.info("=" * 60)
        logger.info("微博热搜交易信号分析器 - 单次执行")
        logger.info(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)
        
        current_data = None
        history_data = []
        
        if not args.skip_scrape:
            log_step("数据采集", "开始采集微博热搜数据...")
            
            scraper = WeiboScraper(config)
            
            current_data = scraper.run()
            
            if not current_data:
                log_error("数据采集", "数据采集失败")
                return result
            
            history_data = scraper.get_history_data()
            logger.info(f"获取到 {len(history_data)} 条历史数据")
        else:
            log_step("数据加载", "跳过数据采集，尝试加载已有数据...")
            
            storage_dir = config.get('data', {}).get('storage_dir', './data')
            
            if os.path.exists(storage_dir):
                data_files = []
                for filename in os.listdir(storage_dir):
                    if filename.startswith('weibo_hot_') and filename.endswith('.json'):
                        filepath = os.path.join(storage_dir, filename)
                        data_files.append(filepath)
                
                if data_files:
                    data_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
                    
                    try:
                        with open(data_files[0], 'r', encoding='utf-8') as f:
                            current_data = json.load(f)
                        logger.info(f"成功加载最新数据: {data_files[0]}")
                    except Exception as e:
                        log_error("数据加载", f"加载数据失败: {e}")
                        return result
                    
                    for filepath in data_files[1:config.get('data', {}).get('history_days', 7)]:
                        try:
                            with open(filepath, 'r', encoding='utf-8') as f:
                                history_data.append(json.load(f))
                        except Exception as e:
                            logger.warning(f"加载历史数据失败: {e}")
                    
                    logger.info(f"获取到 {len(history_data)} 条历史数据")
                else:
                    log_error("数据加载", "没有找到已有数据，请先运行数据采集")
                    return result
            else:
                log_error("数据加载", "数据目录不存在，请先运行数据采集")
                return result
        
        result['current_data'] = current_data
        
        analysis_result = None
        
        if not args.skip_analysis:
            log_step("AI分析", "开始AI分析数据...")
            
            analyzer = AIAnalyzer(config)
            analysis_result = analyzer.run_analysis(current_data, history_data)
            
            if not analysis_result:
                logger.warning("AI分析失败，但继续生成报告")
        
        result['analysis_result'] = analysis_result
        
        html_path = None
        json_path = None
        
        if not args.skip_report:
            log_step("报告生成", "开始生成HTML报告...")
            
            generator = ReportGenerator(config)
            
            html_path = generator.generate_report(current_data, analysis_result)
            result['html_path'] = html_path
            
            if analysis_result:
                json_path = generator.save_analysis_result(analysis_result)
                result['json_path'] = json_path
        
        push_results = push_results_to_channels(config, analysis_result, html_path)
        result['push_results'] = push_results
        
        result['success'] = True
        
        logger.info("=" * 60)
        logger.info("单次执行完成！")
        logger.info("=" * 60)
        
        if html_path:
            logger.info(f"📄 HTML报告: {html_path}")
        
        if json_path:
            logger.info(f"📊 分析结果: {json_path}")
        
        logger.info("\n⚠️  免责声明：本报告仅供参考，不构成任何投资建议。")
        logger.info("      股市有风险，投资需谨慎。")
        
    except Exception as e:
        log_error("主流程", f"执行过程中发生异常: {e}", e)
        result['success'] = False
    
    return result


def push_results_to_channels(config, analysis_result, html_path=None):
    """
    推送结果到各个渠道
    
    Args:
        config: 配置
        analysis_result: 分析结果
        html_path: HTML报告路径
        
    Returns:
        推送结果字典
    """
    push_results = {}
    
    try:
        push_config = config.get('push', {})
        
        if not push_config.get('enabled', False):
            logger.info("推送功能已禁用，跳过")
            return push_results
        
        push_manager = PushManager(push_config)
        
        if not push_manager.is_available():
            logger.info("没有可用的推送器，跳过推送")
            return push_results
        
        if analysis_result:
            title = f"微博热搜交易信号分析 - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            
            log_step("推送", "开始推送分析结果...")
            
            push_results = push_manager.push_analysis_card(
                title=title,
                analysis_result=analysis_result,
                html_path=html_path
            )
            
            for pusher_name, success in push_results.items():
                log_push_result(pusher_name, success, "分析卡片推送")
        else:
            logger.warning("没有分析结果，跳过推送")
    
    except Exception as e:
        log_error("推送", f"推送过程中发生异常: {e}", e)
    
    return push_results


def create_task_func(config, args):
    """
    创建任务函数（用于调度器）
    
    Args:
        config: 配置
        args: 命令行参数
        
    Returns:
        任务函数
    """
    def task_func():
        return run_once(config, args)
    
    return task_func


def run_test_mode(config):
    """测试模式 - 使用模拟数据"""
    logger.info("\n[测试模式] 准备模拟数据...")
    
    current_data = {
        'timestamp': datetime.now().isoformat(),
        'hot_list': [
            {'rank': 1, 'title': '人工智能技术突破', 'hot': '500万', 'url': '', 'is_market': False},
            {'rank': 2, 'title': '新能源汽车销量创新高', 'hot': '400万', 'url': '', 'is_market': False},
            {'rank': 3, 'title': '央行降准', 'hot': '350万', 'url': '', 'is_market': False},
            {'rank': 4, 'title': '芯片短缺问题缓解', 'hot': '300万', 'url': '', 'is_market': False},
            {'rank': 5, 'title': '消费升级趋势明显', 'hot': '250万', 'url': '', 'is_market': False},
            {'rank': 6, 'title': '5G应用场景拓展', 'hot': '200万', 'url': '', 'is_market': False},
            {'rank': 7, 'title': '医疗健康政策利好', 'hot': '180万', 'url': '', 'is_market': False},
            {'rank': 8, 'title': '教育改革推进', 'hot': '150万', 'url': '', 'is_market': False},
            {'rank': 9, 'title': '环保政策收紧', 'hot': '120万', 'url': '', 'is_market': False},
            {'rank': 10, 'title': '金融科技发展', 'hot': '100万', 'url': '', 'is_market': False}
        ]
    }
    
    history_data = [
        {
            'timestamp': '2026-04-20T10:00:00',
            'hot_list': [
                {'rank': 1, 'title': '新能源汽车销量创新高', 'hot': '380万', 'url': '', 'is_market': False},
                {'rank': 2, 'title': '人工智能技术突破', 'hot': '350万', 'url': '', 'is_market': False},
                {'rank': 3, 'title': '芯片短缺问题', 'hot': '280万', 'url': '', 'is_market': False},
                {'rank': 4, 'title': '房地产政策', 'hot': '250万', 'url': '', 'is_market': False},
                {'rank': 5, 'title': '医疗健康', 'hot': '200万', 'url': '', 'is_market': False}
            ]
        }
    ]
    
    analysis_result = {
        'timestamp': datetime.now().isoformat(),
        'trend_analysis': {
            'new_hot_topics': [
                {'title': '人工智能技术突破', 'reason': 'AI技术取得重大突破，引发广泛关注', 'rank_change': '新上榜'}
            ],
            'rising_topics': [
                {'title': '新能源汽车销量创新高', 'trend': '热度持续上升，连续3天排名前5', 'potential_impact': '可能利好新能源汽车产业链'}
            ],
            'declining_topics': [
                {'title': '房地产政策', 'trend': '热度逐渐下降，已退出前10', 'reason': '政策影响逐渐消化'}
            ],
            'keyword_evolution': {
                'emerging_keywords': ['人工智能', 'AI技术', '技术突破'],
                'fading_keywords': ['房地产', '政策调控'],
                'stable_keywords': ['新能源汽车', '芯片', '消费升级']
            },
            'overall_trend_summary': '整体来看，科技类话题热度持续上升，人工智能和新能源汽车成为当前热点。传统行业话题热度有所下降。'
        },
        'stock_analysis': {
            'stock_opportunities': [
                {
                    'event': '人工智能技术突破',
                    'impact_level': '高',
                    'related_industries': ['人工智能', '计算机', '半导体'],
                    'related_stocks': [
                        {
                            'stock_name': '科大讯飞',
                            'stock_code': '002230',
                            'industry': '计算机应用',
                            'reasoning': '国内AI龙头企业，技术突破将直接利好公司发展',
                            'signal_type': '买入信号',
                            'confidence': 85
                        },
                        {
                            'stock_name': '寒武纪',
                            'stock_code': '688256',
                            'industry': '半导体',
                            'reasoning': 'AI芯片设计企业，技术突破将提升市场需求预期',
                            'signal_type': '买入信号',
                            'confidence': 80
                        }
                    ],
                    'analysis': '人工智能技术取得重大突破，将推动整个AI产业链的发展。从算法到算力，从应用到数据，各个环节都将受益。建议关注具有核心技术优势的龙头企业。',
                    'risk_warning': '技术突破的商业化落地仍需时间，短期市场反应可能过度。建议投资者保持理性，关注公司基本面和估值水平。',
                    'signal_type': '买入信号'
                },
                {
                    'event': '新能源汽车销量创新高',
                    'impact_level': '高',
                    'related_industries': ['新能源汽车', '锂电池', '汽车零部件'],
                    'related_stocks': [
                        {
                            'stock_name': '比亚迪',
                            'stock_code': '002594',
                            'industry': '汽车整车',
                            'reasoning': '国内新能源汽车龙头，销量数据亮眼',
                            'signal_type': '买入信号',
                            'confidence': 82
                        },
                        {
                            'stock_name': '宁德时代',
                            'stock_code': '300750',
                            'industry': '电力设备',
                            'reasoning': '全球动力电池龙头，将直接受益于新能源汽车销量增长',
                            'signal_type': '买入信号',
                            'confidence': 78
                        }
                    ],
                    'analysis': '新能源汽车销量持续创新高，表明行业景气度仍然较高。政策支持和技术进步推动行业快速发展，建议关注产业链优质企业。',
                    'risk_warning': '行业竞争激烈，价格战可能影响企业盈利。同时，原材料价格波动也会带来成本压力。',
                    'signal_type': '买入信号'
                },
                {
                    'event': '央行降准',
                    'impact_level': '高',
                    'related_industries': ['银行', '房地产', '金融'],
                    'related_stocks': [
                        {
                            'stock_name': '招商银行',
                            'stock_code': '600036',
                            'industry': '银行',
                            'reasoning': '降准将释放流动性，利好银行板块',
                            'signal_type': '观察信号',
                            'confidence': 65
                        }
                    ],
                    'analysis': '央行降准将释放流动性，对市场整体形成利好。但需关注政策落地效果和市场反应。',
                    'risk_warning': '降准利好可能已被市场提前消化，需警惕利好出尽的风险。',
                    'signal_type': '观察信号'
                }
            ],
            'market_sentiment': {
                'overall_sentiment': '乐观',
                'sentiment_reason': '科技类热点持续，市场情绪较为积极。但需警惕热点切换带来的波动风险。',
                'hot_industries': ['人工智能', '新能源汽车', '半导体', '消费电子']
            },
            'summary': '综合分析，当前市场热点集中在科技领域，人工智能和新能源汽车相关话题热度较高。建议投资者关注相关行业的优质标的，但需保持理性，做好风险控制。'
        }
    }
    
    logger.info("\n[测试模式] 生成HTML报告...")
    
    generator = ReportGenerator(config)
    
    html_path = generator.generate_report(current_data, analysis_result)
    json_path = generator.save_analysis_result(analysis_result)
    
    push_config = config.get('push', {})
    if push_config.get('enabled', False):
        logger.info("\n[测试模式] 推送测试结果...")
        push_manager = PushManager(push_config)
        if push_manager.is_available():
            title = f"[测试] 微博热搜交易信号分析 - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            push_manager.push_analysis_card(title, analysis_result, html_path)
    
    logger.info("\n" + "=" * 60)
    logger.info("测试模式执行完成！")
    logger.info("=" * 60)
    
    if html_path:
        logger.info(f"\n📄 HTML报告: {html_path}")
    
    if json_path:
        logger.info(f"📊 分析结果: {json_path}")
    
    logger.info("\n⚠️  免责声明：本报告仅供参考，不构成任何投资建议。")
    logger.info("      股市有风险，投资需谨慎。")
    
    return {
        'success': True,
        'html_path': html_path,
        'json_path': json_path,
        'analysis_result': analysis_result,
        'current_data': current_data
    }


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='微博热搜交易信号分析器')
    parser.add_argument('-c', '--config', default='config.json', help='配置文件路径')
    parser.add_argument('--skip-scrape', action='store_true', help='跳过数据采集，使用已有数据')
    parser.add_argument('--skip-analysis', action='store_true', help='跳过AI分析')
    parser.add_argument('--skip-report', action='store_true', help='跳过报告生成')
    parser.add_argument('--skip-push', action='store_true', help='跳过推送')
    parser.add_argument('--test', action='store_true', help='使用测试数据运行')
    parser.add_argument('--daemon', action='store_true', help='以守护进程模式运行（定时调度）')
    parser.add_argument('--once', action='store_true', help='仅执行一次（默认）')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("微博热搜交易信号分析器")
    print(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    print("\n[1/4] 加载配置文件...")
    config = load_config(args.config)
    
    if not config:
        print("配置加载失败，程序退出")
        sys.exit(1)
    
    print(f"配置文件加载成功: {args.config}")
    
    init_logging(config)
    
    if args.test:
        logger.info("\n⚠️  测试模式：将使用模拟数据运行")
        run_test_mode(config)
        return
    
    if not check_config(config):
        print("配置不完整，程序退出")
        sys.exit(1)
    
    use_scheduler = args.daemon and not args.once
    
    if use_scheduler:
        logger.info("启动调度器模式...")
        task_func = create_task_func(config, args)
        schedule_config = config.get('schedule', {})
        
        if not schedule_config.get('enabled', True):
            logger.warning("调度器已在配置中禁用，退出")
            sys.exit(0)
        
        scheduler = TaskScheduler(schedule_config, task_func)
        scheduler.start()
    else:
        logger.info("启动单次执行模式...")
        result = run_once(config, args)
        
        if result['success']:
            logger.info("执行成功！")
            sys.exit(0)
        else:
            logger.error("执行失败！")
            sys.exit(1)


if __name__ == '__main__':
    main()
