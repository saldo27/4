"""
Analytics Widgets for AI-Powered Workload Demand Forecasting System

This module provides Kivy UI components for displaying predictive analytics,
demand forecasts, and optimization recommendations in the scheduler interface.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import json

from kivy.uix.boxlayout import BoxLayout
from kivy.uix.gridlayout import GridLayout
from kivy.uix.label import Label
from kivy.uix.button import Button
from kivy.uix.popup import Popup
from kivy.uix.scrollview import ScrollView
from kivy.uix.textinput import TextInput
from kivy.uix.progressbar import ProgressBar
from kivy.uix.tabbedpanel import TabbedPanel, TabbedPanelItem
from kivy.uix.accordion import Accordion, AccordionItem
from kivy.clock import Clock
from kivy.graphics import Color, Rectangle
from kivy.core.text import LabelBase

try:
    import matplotlib.pyplot as plt
    import matplotlib.patches as patches
    from matplotlib.backends.backend_kivy import FigureCanvasKivy
    import numpy as np
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    logging.warning("Matplotlib not available. Charts will be text-based.")


class PredictiveAnalyticsDashboard(BoxLayout):
    """Main dashboard widget for predictive analytics"""
    
    def __init__(self, scheduler, predictive_analytics_engine=None, **kwargs):
        super().__init__(orientation='vertical', **kwargs)
        
        self.scheduler = scheduler
        self.predictive_analytics = predictive_analytics_engine
        self.refresh_interval = 60  # seconds
        
        # Create dashboard layout
        self._build_dashboard()
        
        # Schedule regular updates
        self.update_event = Clock.schedule_interval(self.update_dashboard, self.refresh_interval)
        
        logging.info("PredictiveAnalyticsDashboard initialized")
    
    def _build_dashboard(self):
        """Build the main dashboard layout"""
        # Header
        header = BoxLayout(orientation='horizontal', size_hint_y=None, height=50)
        
        title_label = Label(
            text='Predictive Analytics Dashboard',
            font_size='18sp',
            size_hint_x=0.7,
            halign='left'
        )
        title_label.bind(size=title_label.setter('text_size'))
        header.add_widget(title_label)
        
        # Control buttons
        refresh_btn = Button(
            text='Refresh',
            size_hint_x=0.15,
            on_press=self.manual_refresh
        )
        header.add_widget(refresh_btn)
        
        settings_btn = Button(
            text='Settings',
            size_hint_x=0.15,
            on_press=self.show_settings
        )
        header.add_widget(settings_btn)
        
        self.add_widget(header)
        
        # Main content area with tabs
        self.main_tabs = TabbedPanel(do_default_tab=False)
        
        # Overview tab
        overview_tab = TabbedPanelItem(text='Overview')
        overview_tab.content = self._create_overview_panel()
        self.main_tabs.add_widget(overview_tab)
        
        # Forecasts tab
        forecasts_tab = TabbedPanelItem(text='Forecasts')
        forecasts_tab.content = self._create_forecasts_panel()
        self.main_tabs.add_widget(forecasts_tab)
        
        # Optimization tab
        optimization_tab = TabbedPanelItem(text='Optimization')
        optimization_tab.content = self._create_optimization_panel()
        self.main_tabs.add_widget(optimization_tab)
        
        # Historical Data tab
        history_tab = TabbedPanelItem(text='Historical Data')
        history_tab.content = self._create_history_panel()
        self.main_tabs.add_widget(history_tab)
        
        self.add_widget(self.main_tabs)
        
        # Status bar
        self.status_bar = Label(
            text='Loading analytics...',
            size_hint_y=None,
            height=30,
            color=(0.7, 0.7, 0.7, 1)
        )
        self.add_widget(self.status_bar)
    
    def _create_overview_panel(self) -> BoxLayout:
        """Create the overview panel with key metrics"""
        panel = BoxLayout(orientation='vertical', padding=10, spacing=10)
        
        # Key metrics grid
        metrics_grid = GridLayout(cols=2, spacing=10, size_hint_y=None)
        metrics_grid.bind(minimum_height=metrics_grid.setter('height'))
        
        # Metric cards
        self.overview_widgets = {}
        
        # System Status Card
        status_card = self._create_metric_card('System Status', 'Initializing...', 'lightblue')
        self.overview_widgets['system_status'] = status_card
        metrics_grid.add_widget(status_card)
        
        # Forecast Confidence Card
        confidence_card = self._create_metric_card('Forecast Confidence', 'N/A', 'lightgreen')
        self.overview_widgets['forecast_confidence'] = confidence_card
        metrics_grid.add_widget(confidence_card)
        
        # Predicted Fill Rate Card
        fill_rate_card = self._create_metric_card('Predicted Fill Rate', 'N/A', 'lightyellow')
        self.overview_widgets['predicted_fill_rate'] = fill_rate_card
        metrics_grid.add_widget(fill_rate_card)
        
        # Risk Level Card
        risk_card = self._create_metric_card('Risk Level', 'N/A', 'lightcoral')
        self.overview_widgets['risk_level'] = risk_card
        metrics_grid.add_widget(risk_card)
        
        panel.add_widget(metrics_grid)
        
        # Recent insights section
        insights_label = Label(
            text='Recent Insights',
            size_hint_y=None,
            height=30,
            font_size='16sp',
            halign='left'
        )
        insights_label.bind(size=insights_label.setter('text_size'))
        panel.add_widget(insights_label)
        
        # Insights scroll view
        insights_scroll = ScrollView()
        self.insights_layout = BoxLayout(orientation='vertical', size_hint_y=None, spacing=5)
        self.insights_layout.bind(minimum_height=self.insights_layout.setter('height'))
        insights_scroll.add_widget(self.insights_layout)
        panel.add_widget(insights_scroll)
        
        return panel
    
    def _create_metric_card(self, title: str, value: str, bg_color: str) -> BoxLayout:
        """Create a metric display card"""
        card = BoxLayout(orientation='vertical', size_hint_y=None, height=100, padding=5)
        
        # Add background color
        with card.canvas.before:
            Color(0.9, 0.9, 0.9, 1)  # Light gray background
            self.rect = Rectangle(size=card.size, pos=card.pos)
            card.bind(size=self._update_rect, pos=self._update_rect)
        
        title_label = Label(
            text=title,
            font_size='14sp',
            size_hint_y=0.4,
            color=(0.2, 0.2, 0.2, 1)
        )
        card.add_widget(title_label)
        
        value_label = Label(
            text=value,
            font_size='20sp',
            size_hint_y=0.6,
            bold=True,
            color=(0.1, 0.1, 0.1, 1)
        )
        card.add_widget(value_label)
        
        # Store reference to value label for updates
        card.value_label = value_label
        
        return card
    
    def _update_rect(self, instance, value):
        """Update rectangle size for metric cards"""
        if hasattr(instance, 'rect'):
            instance.rect.pos = instance.pos
            instance.rect.size = instance.size
    
    def _create_forecasts_panel(self) -> BoxLayout:
        """Create the forecasts panel with predictions and charts"""
        panel = BoxLayout(orientation='vertical', padding=10, spacing=10)
        
        # Forecast controls
        controls = BoxLayout(orientation='horizontal', size_hint_y=None, height=40, spacing=10)
        
        controls.add_widget(Label(text='Forecast Period:', size_hint_x=0.3))
        
        self.forecast_days_input = TextInput(
            text='30',
            multiline=False,
            input_filter='int',
            size_hint_x=0.2
        )
        controls.add_widget(self.forecast_days_input)
        
        generate_btn = Button(
            text='Generate Forecast',
            size_hint_x=0.3,
            on_press=self.generate_forecast
        )
        controls.add_widget(generate_btn)
        
        panel.add_widget(controls)
        
        # Forecast display area
        if MATPLOTLIB_AVAILABLE:
            # Chart area
            self.forecast_chart_area = BoxLayout(size_hint_y=0.6)
            panel.add_widget(self.forecast_chart_area)
        
        # Forecast summary
        summary_label = Label(
            text='Forecast Summary',
            size_hint_y=None,
            height=30,
            font_size='16sp',
            halign='left'
        )
        summary_label.bind(size=summary_label.setter('text_size'))
        panel.add_widget(summary_label)
        
        # Forecast details scroll view
        forecast_scroll = ScrollView()
        self.forecast_details = BoxLayout(orientation='vertical', size_hint_y=None, spacing=5)
        self.forecast_details.bind(minimum_height=self.forecast_details.setter('height'))
        forecast_scroll.add_widget(self.forecast_details)
        panel.add_widget(forecast_scroll)
        
        return panel
    
    def _create_optimization_panel(self) -> BoxLayout:
        """Create the optimization panel with recommendations"""
        panel = BoxLayout(orientation='vertical', padding=10, spacing=10)
        
        # Optimization controls
        controls = BoxLayout(orientation='horizontal', size_hint_y=None, height=40, spacing=10)
        
        optimize_btn = Button(
            text='Run Optimization Analysis',
            size_hint_x=0.4,
            on_press=self.run_optimization_analysis
        )
        controls.add_widget(optimize_btn)
        
        auto_optimize_btn = Button(
            text='Auto-Optimize',
            size_hint_x=0.3,
            on_press=self.auto_optimize
        )
        controls.add_widget(auto_optimize_btn)
        
        panel.add_widget(controls)
        
        # Optimization results using accordion
        self.optimization_accordion = Accordion(orientation='vertical')
        
        # Recommendations section
        recommendations_item = AccordionItem(title='Optimization Recommendations')
        self.recommendations_layout = BoxLayout(orientation='vertical', spacing=5, padding=5)
        recommendations_scroll = ScrollView()
        recommendations_scroll.add_widget(self.recommendations_layout)
        recommendations_item.add_widget(recommendations_scroll)
        self.optimization_accordion.add_widget(recommendations_item)
        
        # Conflicts section
        conflicts_item = AccordionItem(title='Predicted Conflicts')
        self.conflicts_layout = BoxLayout(orientation='vertical', spacing=5, padding=5)
        conflicts_scroll = ScrollView()
        conflicts_scroll.add_widget(self.conflicts_layout)
        conflicts_item.add_widget(conflicts_scroll)
        self.optimization_accordion.add_widget(conflicts_item)
        
        # Early warnings section
        warnings_item = AccordionItem(title='Early Warnings')
        self.warnings_layout = BoxLayout(orientation='vertical', spacing=5, padding=5)
        warnings_scroll = ScrollView()
        warnings_scroll.add_widget(self.warnings_layout)
        warnings_item.add_widget(warnings_scroll)
        self.optimization_accordion.add_widget(warnings_item)
        
        panel.add_widget(self.optimization_accordion)
        
        return panel
    
    def _create_history_panel(self) -> BoxLayout:
        """Create the historical data panel"""
        panel = BoxLayout(orientation='vertical', padding=10, spacing=10)
        
        # Historical data controls
        controls = BoxLayout(orientation='horizontal', size_hint_y=None, height=40, spacing=10)
        
        collect_btn = Button(
            text='Collect Current Data',
            size_hint_x=0.3,
            on_press=self.collect_historical_data
        )
        controls.add_widget(collect_btn)
        
        export_btn = Button(
            text='Export Data',
            size_hint_x=0.2,
            on_press=self.export_historical_data
        )
        controls.add_widget(export_btn)
        
        panel.add_widget(controls)
        
        # Historical data summary
        summary_label = Label(
            text='Historical Data Summary',
            size_hint_y=None,
            height=30,
            font_size='16sp',
            halign='left'
        )
        summary_label.bind(size=summary_label.setter('text_size'))
        panel.add_widget(summary_label)
        
        # Historical data display
        history_scroll = ScrollView()
        self.history_layout = BoxLayout(orientation='vertical', size_hint_y=None, spacing=5)
        self.history_layout.bind(minimum_height=self.history_layout.setter('height'))
        history_scroll.add_widget(self.history_layout)
        panel.add_widget(history_scroll)
        
        return panel
    
    def update_dashboard(self, dt=None):
        """Update dashboard with latest data"""
        try:
            if not self.predictive_analytics or not self.predictive_analytics.is_enabled():
                self.status_bar.text = 'Predictive analytics disabled'
                return
            
            # Update overview metrics
            self._update_overview_metrics()
            
            # Update insights
            self._update_insights()
            
            # Update historical data summary
            self._update_historical_summary()
            
            self.status_bar.text = f'Last updated: {datetime.now().strftime("%H:%M:%S")}'
            
        except Exception as e:
            logging.error(f"Error updating dashboard: {e}")
            self.status_bar.text = f'Update error: {str(e)[:50]}...'
    
    def _update_overview_metrics(self):
        """Update overview metric cards"""
        try:
            # System status
            status = 'Active' if self.predictive_analytics.is_enabled() else 'Disabled'
            self.overview_widgets['system_status'].value_label.text = status
            
            # Get analytics summary
            summary = self.predictive_analytics.get_analytics_summary()
            
            # Update forecast confidence
            if self.predictive_analytics.latest_forecasts:
                confidence = self.predictive_analytics.latest_forecasts.get('confidence_score', {})
                confidence_value = confidence.get('overall_score', 0)
                self.overview_widgets['forecast_confidence'].value_label.text = f'{confidence_value:.1%}'
                
                # Update predicted fill rate
                predictions = self.predictive_analytics.latest_forecasts.get('predictions', {})
                ensemble = predictions.get('ensemble', {})
                if 'fill_rates' in ensemble:
                    avg_fill_rate = sum(ensemble['fill_rates']) / len(ensemble['fill_rates'])
                    self.overview_widgets['predicted_fill_rate'].value_label.text = f'{avg_fill_rate:.1%}'
                
                # Update risk level
                risk_assessment = self.predictive_analytics.latest_forecasts.get('risk_assessment', {})
                risk_level = risk_assessment.get('overall_risk_level', 'Unknown')
                self.overview_widgets['risk_level'].value_label.text = risk_level.title()
            
        except Exception as e:
            logging.error(f"Error updating overview metrics: {e}")
    
    def _update_insights(self):
        """Update insights display"""
        try:
            self.insights_layout.clear_widgets()
            
            insights_result = self.predictive_analytics.get_predictive_insights()
            if insights_result.get('status') == 'success':
                insights = insights_result.get('insights', {})
                key_insights = insights.get('key_insights', [])
                
                for insight in key_insights[:5]:  # Show top 5 insights
                    insight_label = Label(
                        text=f"• {insight}",
                        size_hint_y=None,
                        height=30,
                        text_size=(None, None),
                        halign='left'
                    )
                    insight_label.bind(width=lambda x, y: setattr(x, 'text_size', (y, None)))
                    self.insights_layout.add_widget(insight_label)
            
        except Exception as e:
            logging.error(f"Error updating insights: {e}")
    
    def _update_historical_summary(self):
        """Update historical data summary"""
        try:
            if not hasattr(self, 'history_layout'):
                return
                
            self.history_layout.clear_widgets()
            
            summary = self.predictive_analytics.historical_data_manager.get_historical_summary()
            
            if summary.get('status') == 'data_available':
                record_count = summary.get('record_count', 0)
                last_record = summary.get('latest_record')
                
                # Record count
                count_label = Label(
                    text=f"Total Records: {record_count}",
                    size_hint_y=None,
                    height=30,
                    halign='left'
                )
                count_label.bind(size=count_label.setter('text_size'))
                self.history_layout.add_widget(count_label)
                
                # Last record info
                if last_record:
                    timestamp = last_record.get('timestamp', 'Unknown')
                    efficiency = last_record.get('efficiency_score', 0)
                    
                    last_label = Label(
                        text=f"Last Record: {timestamp[:16]} (Efficiency: {efficiency:.2%})",
                        size_hint_y=None,
                        height=30,
                        halign='left'
                    )
                    last_label.bind(size=last_label.setter('text_size'))
                    self.history_layout.add_widget(last_label)
            else:
                no_data_label = Label(
                    text="No historical data available",
                    size_hint_y=None,
                    height=30,
                    halign='left'
                )
                no_data_label.bind(size=no_data_label.setter('text_size'))
                self.history_layout.add_widget(no_data_label)
            
        except Exception as e:
            logging.error(f"Error updating historical summary: {e}")
    
    def manual_refresh(self, instance):
        """Manually refresh dashboard"""
        self.update_dashboard()
    
    def show_settings(self, instance):
        """Show analytics settings popup"""
        content = BoxLayout(orientation='vertical', spacing=10, padding=10)
        
        # Refresh interval setting
        interval_layout = BoxLayout(orientation='horizontal')
        interval_layout.add_widget(Label(text='Refresh Interval (seconds):', size_hint_x=0.7))
        interval_input = TextInput(
            text=str(self.refresh_interval),
            multiline=False,
            input_filter='int',
            size_hint_x=0.3
        )
        interval_layout.add_widget(interval_input)
        content.add_widget(interval_layout)
        
        # Auto-collect setting
        auto_collect_layout = BoxLayout(orientation='horizontal')
        auto_collect_layout.add_widget(Label(text='Auto-collect Data:', size_hint_x=0.7))
        auto_collect_btn = Button(
            text='Enabled' if self.predictive_analytics.config.get('auto_collect_data', True) else 'Disabled',
            size_hint_x=0.3
        )
        auto_collect_layout.add_widget(auto_collect_btn)
        content.add_widget(auto_collect_layout)
        
        # Buttons
        button_layout = BoxLayout(orientation='horizontal', size_hint_y=None, height=40)
        
        def apply_settings(instance):
            try:
                new_interval = int(interval_input.text)
                if new_interval > 0:
                    self.refresh_interval = new_interval
                    # Reschedule update event
                    self.update_event.cancel()
                    self.update_event = Clock.schedule_interval(self.update_dashboard, self.refresh_interval)
                popup.dismiss()
            except ValueError:
                pass
        
        apply_btn = Button(text='Apply', on_press=apply_settings)
        cancel_btn = Button(text='Cancel', on_press=lambda x: popup.dismiss())
        
        button_layout.add_widget(apply_btn)
        button_layout.add_widget(cancel_btn)
        content.add_widget(button_layout)
        
        popup = Popup(
            title='Analytics Settings',
            content=content,
            size_hint=(0.6, 0.4)
        )
        popup.open()
    
    def generate_forecast(self, instance):
        """Generate new forecast"""
        try:
            forecast_days = int(self.forecast_days_input.text)
            
            # Show loading indicator
            self.status_bar.text = 'Generating forecast...'
            
            # Generate forecast
            result = self.predictive_analytics.generate_demand_forecasts(forecast_days)
            
            if result.get('status') == 'success':
                self._display_forecast_results(result.get('forecasts', {}))
                self.status_bar.text = f'Forecast generated for {forecast_days} days'
            else:
                self.status_bar.text = f'Forecast failed: {result.get("message", "Unknown error")}'
            
        except ValueError:
            self.status_bar.text = 'Invalid forecast period'
        except Exception as e:
            logging.error(f"Error generating forecast: {e}")
            self.status_bar.text = f'Forecast error: {str(e)[:50]}...'
    
    def _display_forecast_results(self, forecasts: Dict[str, Any]):
        """Display forecast results in the UI"""
        try:
            self.forecast_details.clear_widgets()
            
            # Display forecast metadata
            metadata = forecasts.get('metadata', {})
            methods_used = metadata.get('methods_used', [])
            data_points = metadata.get('data_points_used', 0)
            
            meta_label = Label(
                text=f"Methods: {', '.join(methods_used)} | Data Points: {data_points}",
                size_hint_y=None,
                height=30,
                halign='left'
            )
            meta_label.bind(size=meta_label.setter('text_size'))
            self.forecast_details.add_widget(meta_label)
            
            # Display predictions summary
            predictions = forecasts.get('predictions', {})
            for method, prediction in predictions.items():
                if 'fill_rates' in prediction:
                    fill_rates = prediction['fill_rates']
                    avg_rate = sum(fill_rates) / len(fill_rates)
                    min_rate = min(fill_rates)
                    max_rate = max(fill_rates)
                    
                    pred_label = Label(
                        text=f"{method}: Avg {avg_rate:.1%}, Range {min_rate:.1%}-{max_rate:.1%}",
                        size_hint_y=None,
                        height=30,
                        halign='left'
                    )
                    pred_label.bind(size=pred_label.setter('text_size'))
                    self.forecast_details.add_widget(pred_label)
            
            # Display recommendations
            recommendations = forecasts.get('recommendations', [])
            if recommendations:
                rec_title = Label(
                    text="Recommendations:",
                    size_hint_y=None,
                    height=30,
                    font_size='14sp',
                    bold=True,
                    halign='left'
                )
                rec_title.bind(size=rec_title.setter('text_size'))
                self.forecast_details.add_widget(rec_title)
                
                for rec in recommendations[:5]:  # Show top 5
                    rec_label = Label(
                        text=f"• {rec}",
                        size_hint_y=None,
                        height=40,
                        text_size=(None, None),
                        halign='left'
                    )
                    rec_label.bind(width=lambda x, y: setattr(x, 'text_size', (y, None)))
                    self.forecast_details.add_widget(rec_label)
            
            # Generate chart if matplotlib is available
            if MATPLOTLIB_AVAILABLE and hasattr(self, 'forecast_chart_area'):
                self._create_forecast_chart(forecasts)
            
        except Exception as e:
            logging.error(f"Error displaying forecast results: {e}")
    
    def _create_forecast_chart(self, forecasts: Dict[str, Any]):
        """Create forecast visualization chart"""
        try:
            self.forecast_chart_area.clear_widgets()
            
            predictions = forecasts.get('predictions', {})
            ensemble = predictions.get('ensemble', {})
            
            if 'fill_rates' in ensemble and 'dates' in ensemble:
                fig, ax = plt.subplots(figsize=(10, 6))
                
                dates = ensemble['dates'][:14]  # Show first 2 weeks
                fill_rates = ensemble['fill_rates'][:14]
                
                # Convert date strings to datetime for better plotting
                try:
                    from datetime import datetime as dt
                    date_objects = [dt.strptime(date, '%Y-%m-%d') for date in dates]
                    ax.plot(date_objects, fill_rates, marker='o', linewidth=2, label='Predicted Fill Rate')
                except:
                    # Fallback to simple range if date parsing fails
                    ax.plot(range(len(fill_rates)), fill_rates, marker='o', linewidth=2, label='Predicted Fill Rate')
                    ax.set_xlabel('Days Ahead')
                
                ax.set_ylabel('Fill Rate')
                ax.set_title('Demand Forecast - Next 2 Weeks')
                ax.grid(True, alpha=0.3)
                ax.legend()
                
                # Add threshold lines
                ax.axhline(y=0.8, color='orange', linestyle='--', alpha=0.7, label='Target (80%)')
                ax.axhline(y=0.6, color='red', linestyle='--', alpha=0.7, label='Critical (60%)')
                
                plt.xticks(rotation=45)
                plt.tight_layout()
                
                canvas = FigureCanvasKivy(figure=fig)
                self.forecast_chart_area.add_widget(canvas)
            
        except Exception as e:
            logging.error(f"Error creating forecast chart: {e}")
    
    def run_optimization_analysis(self, instance):
        """Run optimization analysis"""
        try:
            # Clear previous results
            self.recommendations_layout.clear_widgets()
            self.conflicts_layout.clear_widgets()
            self.warnings_layout.clear_widgets()
            
            self.status_bar.text = 'Running optimization analysis...'
            
            # Run predictive optimization if available
            if hasattr(self.scheduler, 'predictive_optimizer'):
                result = self.scheduler.predictive_optimizer.predict_and_optimize()
                
                if result.get('status') == 'success':
                    self._display_optimization_results(result)
                    self.status_bar.text = 'Optimization analysis completed'
                else:
                    self.status_bar.text = f'Optimization failed: {result.get("message", "Unknown error")}'
            else:
                # Get basic optimization suggestions
                suggestions = self.predictive_analytics.get_optimization_suggestions()
                self._display_basic_suggestions(suggestions)
                self.status_bar.text = 'Basic optimization suggestions provided'
            
        except Exception as e:
            logging.error(f"Error running optimization analysis: {e}")
            self.status_bar.text = f'Optimization error: {str(e)[:50]}...'
    
    def _display_optimization_results(self, results: Dict[str, Any]):
        """Display optimization analysis results"""
        try:
            # Display recommendations
            recommendations = results.get('optimization_recommendations', [])
            for rec in recommendations:
                rec_widget = self._create_recommendation_widget(rec)
                self.recommendations_layout.add_widget(rec_widget)
            
            # Display conflicts
            conflicts = results.get('predicted_conflicts', [])
            for conflict in conflicts:
                conflict_widget = self._create_conflict_widget(conflict)
                self.conflicts_layout.add_widget(conflict_widget)
            
            # Display warnings
            warnings = results.get('early_warnings', [])
            for warning in warnings:
                warning_widget = self._create_warning_widget(warning)
                self.warnings_layout.add_widget(warning_widget)
            
        except Exception as e:
            logging.error(f"Error displaying optimization results: {e}")
    
    def _display_basic_suggestions(self, suggestions: List[str]):
        """Display basic optimization suggestions"""
        for suggestion in suggestions:
            suggestion_label = Label(
                text=f"• {suggestion}",
                size_hint_y=None,
                height=40,
                text_size=(None, None),
                halign='left'
            )
            suggestion_label.bind(width=lambda x, y: setattr(x, 'text_size', (y, None)))
            self.recommendations_layout.add_widget(suggestion_label)
    
    def _create_recommendation_widget(self, recommendation: Dict[str, Any]) -> BoxLayout:
        """Create widget for displaying recommendation"""
        widget = BoxLayout(orientation='vertical', size_hint_y=None, height=80, padding=5)
        
        # Priority and type
        header = BoxLayout(orientation='horizontal', size_hint_y=0.4)
        
        priority = recommendation.get('priority', 'medium')
        rec_type = recommendation.get('type', 'general')
        
        priority_label = Label(
            text=f"[{priority.upper()}] {rec_type.replace('_', ' ').title()}",
            size_hint_x=0.7,
            font_size='12sp',
            halign='left'
        )
        priority_label.bind(size=priority_label.setter('text_size'))
        header.add_widget(priority_label)
        
        complexity = recommendation.get('implementation_complexity', 'unknown')
        complexity_label = Label(
            text=f"Complexity: {complexity}",
            size_hint_x=0.3,
            font_size='10sp',
            color=(0.6, 0.6, 0.6, 1)
        )
        header.add_widget(complexity_label)
        
        widget.add_widget(header)
        
        # Action description
        action = recommendation.get('action', 'No action specified')
        action_label = Label(
            text=action,
            size_hint_y=0.6,
            text_size=(None, None),
            halign='left',
            font_size='11sp'
        )
        action_label.bind(width=lambda x, y: setattr(x, 'text_size', (y, None)))
        widget.add_widget(action_label)
        
        return widget
    
    def _create_conflict_widget(self, conflict: Dict[str, Any]) -> BoxLayout:
        """Create widget for displaying conflict"""
        widget = BoxLayout(orientation='vertical', size_hint_y=None, height=60, padding=5)
        
        # Severity and type
        severity = conflict.get('severity', 'medium')
        conflict_type = conflict.get('type', 'unknown')
        
        header_label = Label(
            text=f"[{severity.upper()}] {conflict_type.replace('_', ' ').title()}",
            size_hint_y=0.4,
            font_size='12sp',
            halign='left',
            color=(1, 0.5, 0.5, 1) if severity == 'high' else (1, 1, 0.5, 1)
        )
        header_label.bind(size=header_label.setter('text_size'))
        widget.add_widget(header_label)
        
        # Description
        description = conflict.get('description', 'No description available')
        desc_label = Label(
            text=description,
            size_hint_y=0.6,
            text_size=(None, None),
            halign='left',
            font_size='11sp'
        )
        desc_label.bind(width=lambda x, y: setattr(x, 'text_size', (y, None)))
        widget.add_widget(desc_label)
        
        return widget
    
    def _create_warning_widget(self, warning: Dict[str, Any]) -> BoxLayout:
        """Create widget for displaying warning"""
        widget = BoxLayout(orientation='vertical', size_hint_y=None, height=60, padding=5)
        
        # Warning header
        warning_type = warning.get('type', 'unknown')
        severity = warning.get('severity', 'medium')
        
        header_label = Label(
            text=f"⚠ [{severity.upper()}] {warning_type.replace('_', ' ').title()}",
            size_hint_y=0.4,
            font_size='12sp',
            halign='left',
            color=(1, 0.6, 0, 1)
        )
        header_label.bind(size=header_label.setter('text_size'))
        widget.add_widget(header_label)
        
        # Warning message
        message = warning.get('message', 'No message available')
        message_label = Label(
            text=message,
            size_hint_y=0.6,
            text_size=(None, None),
            halign='left',
            font_size='11sp'
        )
        message_label.bind(width=lambda x, y: setattr(x, 'text_size', (y, None)))
        widget.add_widget(message_label)
        
        return widget
    
    def auto_optimize(self, instance):
        """Run automatic optimization"""
        # This would integrate with the scheduler's optimization features
        self.status_bar.text = 'Auto-optimization not yet implemented'
    
    def collect_historical_data(self, instance):
        """Collect current historical data"""
        try:
            self.status_bar.text = 'Collecting data...'
            result = self.predictive_analytics.collect_and_store_current_data()
            
            if result.get('status') == 'success':
                self.status_bar.text = 'Data collected successfully'
                self._update_historical_summary()
            else:
                self.status_bar.text = f'Data collection failed: {result.get("message", "Unknown error")}'
            
        except Exception as e:
            logging.error(f"Error collecting data: {e}")
            self.status_bar.text = f'Collection error: {str(e)[:50]}...'
    
    def export_historical_data(self, instance):
        """Export historical data"""
        # This would implement data export functionality
        self.status_bar.text = 'Data export not yet implemented'


# Utility function to add analytics to existing screens
def add_predictive_analytics_to_screen(screen, scheduler, predictive_analytics_engine):
    """
    Add predictive analytics widget to an existing screen
    
    Args:
        screen: The Kivy screen to add analytics to
        scheduler: The scheduler instance
        predictive_analytics_engine: The analytics engine
    """
    try:
        analytics_dashboard = PredictiveAnalyticsDashboard(
            scheduler=scheduler,
            predictive_analytics_engine=predictive_analytics_engine
        )
        
        screen.add_widget(analytics_dashboard)
        logging.info("Predictive analytics dashboard added to screen")
        
        return analytics_dashboard
        
    except Exception as e:
        logging.error(f"Error adding analytics to screen: {e}")
        return None


# Integration helper for main application
def initialize_analytics_widgets():
    """Initialize analytics widgets for the main application"""
    return {
        'PredictiveAnalyticsDashboard': PredictiveAnalyticsDashboard,
        'add_predictive_analytics_to_screen': add_predictive_analytics_to_screen
    }