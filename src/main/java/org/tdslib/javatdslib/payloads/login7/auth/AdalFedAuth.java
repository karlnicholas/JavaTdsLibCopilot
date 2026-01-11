package org.tdslib.javatdslib.payloads.login7.auth;

import java.nio.ByteBuffer;

/**
 * ADAL-based federated authentication extension payload builder.
 */
public final class AdalFedAuth extends FedAuth {
    private final boolean echo;
    private final AdalWorkflow workflow;

    /**
     * Create an AdalFedAuth instance.
     *
     * @param workflow ADAL workflow selection
     * @param echo     whether server should echo challenge
     */
    public AdalFedAuth(final AdalWorkflow workflow, final boolean echo) {
        this.workflow = workflow;
        this.echo = echo;
    }

    /**
     * Build the extension as a ByteBuffer ready to be appended to the LOGIN7 payload.
     *
     * @return ByteBuffer containing the ADAL extension (little-endian, flipped)
     */
    @Override
    public ByteBuffer toByteBuffer() {
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
