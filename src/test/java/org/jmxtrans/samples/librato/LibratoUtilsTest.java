package org.jmxtrans.samples.librato;

import org.junit.Test;

import java.util.List;

/**
 * @author <a href="mailto:cleclerc@xebia.fr">Cyrille Le Clerc</a>
 */
public class LibratoUtilsTest {

    @Test
    public void testListMetrics() throws Exception {
        LibratoUtils libratoUtils = new LibratoUtils();
        List<String> metrics = libratoUtils.listMetrics();
        System.out.println(metrics);
        libratoUtils.deleteMetrics(metrics);
    }
}
