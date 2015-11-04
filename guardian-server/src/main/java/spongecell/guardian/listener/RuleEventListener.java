package spongecell.guardian.listener;

import lombok.extern.slf4j.Slf4j;

import org.kie.api.definition.rule.Rule;
import org.kie.api.event.rule.AfterMatchFiredEvent;
import org.kie.api.event.rule.BeforeMatchFiredEvent;
import org.kie.api.event.rule.DefaultAgendaEventListener;
import org.kie.api.event.rule.MatchCreatedEvent;

/**
 * @author jbrinnand
 */
@Slf4j
public class RuleEventListener extends DefaultAgendaEventListener {
	@Override
	public void beforeMatchFired(BeforeMatchFiredEvent event) {
		final Rule rule = event.getMatch().getRule();
		log.info("BeforeMatchFiredEvent - Rule is: {} ", rule.getName());
		log.trace(event.getClass().getSimpleName());
	};
	
	@Override
	public void matchCreated(MatchCreatedEvent event) {
		final Rule rule = event.getMatch().getRule();
		log.info("MatchCreated event - Rule - package : {}, name: {} , declarations: {} ",
			rule.getPackageName(), rule.getName(), 
			event.getMatch().getDeclarationIds().toString());
	};

	@Override
	public void afterMatchFired(AfterMatchFiredEvent event) {
		final Rule rule = event.getMatch().getRule();
		log.info("AfterMatchFiredEvent - Rule name: {}, id : {}", rule.getName(), rule.getId());
		log.trace(event.getClass().getSimpleName());
	}
}
