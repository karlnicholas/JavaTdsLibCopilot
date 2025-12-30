package org.tdslib.javatdslib.payloads.login7.auth;

import java.nio.ByteBuffer;

public final class ADALFedAuth extends FedAuth {
    private final boolean echo;
    private final ADALWorkflow workflow;

    public ADALFedAuth(ADALWorkflow workflow, boolean echo) {
        this.workflow = workflow;
        this.echo = echo;
    }

    @Override
    public ByteBuffer getBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(7);
        buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        buffer.put(FeatureId);
        buffer.putInt(2); // size (uint32)
        byte options = (byte) (LibraryADAL | (echo ? FedAuthEchoYes : FedAuthEchoNo));
        buffer.put(options);
        buffer.put(workflow.getValue());
        buffer.flip();
        return buffer;
    }
}
